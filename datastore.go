package store

import (
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/slpereira/vero-datastore/model"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// VeroStore implements the access to the datastore, but include some cache for some objects
type VeroStore struct {
	log               *zap.Logger
	dsClient          *datastore.Client
	psClient          *pubsub.Client
	stClient          *storage.Client
	etcd              *VeroEtcdClient
	projectID         string
	cache             *Cache
	pathExpiration    time.Duration
	namespaceIndex    string
	topicIndexing     *pubsub.Topic
	topicInvoice      *pubsub.Topic
	topicFallbackEtcd *pubsub.Topic
	topicDelete       *pubsub.Topic
	doNotAddPath      bool
	versioning        bool
	doNotIndex        bool
	doNotLoadInvoice  bool
}

func NewVeroStore(projectID string, redisAddress []string, redisPwd string,
	etcdEndpoints []string, etcdUser, etcdPwd string, log *zap.Logger, versioning bool,
	doNotIndex bool, doNotAddPath bool, doNotLoadInvoice bool,
	topicIndexing string, topicInvoice string, topicFallbackEtcd string,
	topicDelete string, namespaceIndex string) (*VeroStore, error) {
	dsClient, err := datastore.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, err
	}

	psClient, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		dsClient.Close()
		return nil, err
	}

	stClient, err := storage.NewClient(context.Background())
	if err != nil {
		dsClient.Close()
		psClient.Close()
		return nil, err
	}

	pathExpStr := os.Getenv("PATH_CACHE_TTL")

	pathExpiration, err := time.ParseDuration(pathExpStr)

	if err != nil {
		pathExpiration = 0
	}

	etcd, err := NewVeroEtcdClient(etcdEndpoints, etcdUser, etcdPwd, log)
	if err != nil {
		dsClient.Close()
		psClient.Close()
		stClient.Close()
		return nil, err
	}

	return &VeroStore{dsClient: dsClient,
		psClient:          psClient,
		projectID:         projectID,
		cache:             NewCache(redisAddress, redisPwd, true),
		pathExpiration:    pathExpiration,
		etcd:              etcd,
		log:               log,
		versioning:        versioning,
		doNotAddPath:      doNotAddPath,
		doNotIndex:        doNotIndex,
		doNotLoadInvoice:  doNotLoadInvoice,
		topicIndexing:     psClient.Topic(topicIndexing),
		topicInvoice:      psClient.Topic(topicInvoice),
		// TODO(silvio) avoid create these two topics references if they cannot be used in the flow
		topicFallbackEtcd: psClient.Topic(topicFallbackEtcd),
		topicDelete:       psClient.Topic(topicDelete),
		namespaceIndex:    namespaceIndex,
	}, nil
}

func (s *VeroStore) Close() error {
	s.topicFallbackEtcd.Stop()
	s.topicInvoice.Stop()
	s.topicIndexing.Stop()
	s.psClient.Close()
	s.dsClient.Close()
	s.stClient.Close()
	s.etcd.Close()
	s.cache.Close()
	return nil
}

func (s *VeroStore) GetNode(ID string) (*model.Node, error) {
	s.log.Debug("datastore:node:get", zap.String("id", ID))
	key := datastore.NameKey("Node", ID, nil)
	var n model.Node
	err := s.dsClient.Get(context.Background(), key, &n)
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil, nil
		} else {
			return nil, err
		}
	} else {
		return &n, nil
	}
}

func (s *VeroStore) GetNodeVersion(ID string) (*model.NodeVersion, error) {
	s.log.Debug("datastore:node-version:get", zap.String("id", ID))
	key := datastore.NameKey("NodeVersion", ID, nil)
	var n model.NodeVersion
	err := s.dsClient.Get(context.Background(), key, &n)
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil, nil
		} else {
			return nil, err
		}
	} else {
		//
		if len(n.Chunks) == 1 && n.Chunks[0] == nil {
			n.Chunks = nil
		}
		return &n, nil
	}
}

func (s *VeroStore) PutNode(n *model.Node) error {
	s.log.Debug("datastore:node:put", zap.String("id", n.ID))
	key := datastore.NameKey("Node", n.ID, nil)
	_, err := s.dsClient.Put(context.Background(), key, &n)
	if err != nil {
		return err
	}
	return nil
}

func (s *VeroStore) PutNodeVersion(n *model.NodeVersion) error {
	s.log.Debug("datastore:node-version:put", zap.String("id", n.ID))
	key := datastore.NameKey("NodeVersion", n.ID, nil)
	_, err := s.dsClient.Put(context.Background(), key, n)
	return err
}

// AddFileToVero add a new file to vero dora doc structure
func (s *VeroStore) AddFileToVero(ctx context.Context, event model.GCSEvent) error {
	s.log.Info("add file to vero", zap.String("name", event.Name),
		zap.String("bucket", event.Bucket))
	mStart := time.Now()
	// if file is zero size, discard them
	if event.Size == "0" {
		s.log.Warn("zero size file", zap.String("name", event.Name),
			zap.String("bucket", event.Bucket))
		// delete from storage
		return s.sendMessageToDeleteTopic(event.Bucket, event.Name)
	}
	cs, err := Checksum(event.MD5Hash)
	if err != nil {
		return err
	}
	size, err := strconv.Atoi(event.Size)
	if err != nil {
		return err
	}
	_, err = s.dsClient.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		// Node
		nodeID := filepath.Join("/", event.Name)
		nodeKey := datastore.NameKey("Node", nodeID, nil)
		var n model.Node
		var nv model.NodeVersion
		s.log.Debug("searching for node", zap.String("name", event.Name))
		start := time.Now()
		err := tx.Get(nodeKey, &n)
		s.log.Info("node searched", zap.String("name", event.Name), zap.Duration("time", time.Since(start)))
		if err != nil && err != datastore.ErrNoSuchEntity {
			return err
		}
		delta := 0
		// New Node
		if err == datastore.ErrNoSuchEntity {
			n.Name = filepath.Base(nodeID)
			n.ID = nodeID
			n.NodeType = model.DOCUMENT_TYPE
			n.Path = filepath.Dir(nodeID)
			n.Store = event.Bucket
			n.ContentType = event.ContentType
			n.ContentLength = size
			n.CreatedDate = event.TimeCreated.Format(time.RFC3339)
			n.LastModifiedDate = event.Updated.Format(time.RFC3339)
			urlEncoded, err := url.ParseRequestURI(fmt.Sprintf("gs://%s/%s", event.Bucket, event.Name))
			if err != nil {
				return err
			}
			n.Uri = urlEncoded.String()
			n.StorageClass = "Standard"
			n.ActiveVersionNumber = 1
			n.Checksum = cs
			if event.Metadata != nil {
				n.Metadata = model.NewMetadata(event.Metadata)
			}
			n.Owner = event.Bucket
			s.log.Debug("checking path", zap.String("path", n.Path), zap.String("name", event.Name))
			start = time.Now()
			if !s.doNotAddPath {
				// add path only if the file is completely new, otherwise the path already exists
				if err = s.addPathInternally(n.Path, tx); err != nil {
					return err
				}
			}
			s.log.Info("checked path", zap.String("path", n.Path), zap.String("name", event.Name), zap.Duration("time", time.Now().Sub(start)))
		} else {
			// same file???
			if cs == n.Checksum {
				s.log.Warn("same checksum", zap.String("name", event.Name), zap.String("bucket", event.Bucket))
				return s.sendMessageToDeleteTopic(event.Bucket, event.Name)
			}
			// TODO(silvio) if the new file is replacing an existing file in the bucket before the compression being executed, it is not a new version
			// the new version happens only if the current file is already compressed in the destination bucket
			// is necessary to have some approach to guarantee isolation for compression process and the current file in the bucket
			// that is, the file cannot be processed while is being updated

			// Node exists, we are updating the file, checking versioning and if the file really changed comparing the Checksum
			if s.versioning {
				n.ActiveVersionNumber++
				s.log.Info("new version", zap.String("name", event.Name), zap.String("bucket", event.Bucket), zap.Int("version", n.ActiveVersionNumber))
			} else {
				s.log.Warn("versioning is not enabled", zap.String("name", event.Name), zap.String("bucket", event.Bucket))
				return s.sendMessageToDeleteTopic(event.Bucket, event.Name)
			}
			n.Store = event.Bucket
			n.ContentType = event.ContentType
			n.ContentLength = size
			n.LastModifiedDate = event.Updated.Format(time.RFC3339)
			if event.Metadata != nil {
				n.Metadata = model.NewMetadata(event.Metadata)
			}
			n.Checksum = cs
		}

		// New Node Version
		nvID := fmt.Sprintf("%s/%d", nodeID, n.ActiveVersionNumber)
		nvKey := datastore.NameKey("NodeVersion", nvID, nil)

		nv = model.NodeVersion{
			ID:            nvID,
			NodeID:        nodeID,
			ContentType:   n.ContentType,
			ContentLength: n.ContentLength,
			Store:         n.Store,
			CreatedDate:   event.TimeCreated.Format(time.RFC3339),
			Deleted:       false,
			VersionNumber: n.ActiveVersionNumber,
			StorageClass:  n.StorageClass,
			Uri:           n.Uri,
			Checksum:      cs,
			Chunks:        nil,
		}
		// add everything

		s.log.Debug("adding node version and node", zap.String("node-version", nv.ID),
			zap.String("node", n.ID), zap.String("name", event.Name))
		start = time.Now()
		// add NV
		_, err = tx.PutMulti([]*datastore.Key{nodeKey, nvKey}, []interface{}{&n, &nv})
		if err != nil {
			return err
		}
		s.log.Info("added node version and node", zap.String("node-version", nv.ID),
			zap.String("node", n.ID), zap.Duration("time", time.Since(start)), zap.String("name", event.Name))

		// TODO(silvio) the following operations must be executed outside the datastore transaction???
		// * add hold to the file
		// * send message to topics for indexing and invoice loader
		// * add data to etcd

		// add the holds to the file
		if err := s.addHoldToFile(event.Bucket, event.Name); err != nil {
			return err
		}
		g := new(errgroup.Group)
		// must index the metadata in the elastic
		if !s.doNotIndex {
			// check if there is some metadata telling the process to not index
			_, ok := event.Metadata["DoNotIndex"]
			if !ok {
				g.Go(func() error {
					return s.sendMessageToIndexTopic(&n)
				})
			} else {
				s.log.Warn("file has metadata DoNotIndex", zap.String("name", event.Name))
			}
		}
		// send message to invoice topic if appropriated
		if !s.doNotLoadInvoice {
			_, ok := event.Metadata["DoNotLoadInvoice"]
			if !ok {
				g.Go(func() error {
					return s.sendMessageToInvoiceLoaderTopic(&n)
				})
			} else {
				s.log.Warn("file has metadata DoNotLoadInvoice", zap.String("name", event.Name))
			}
		}
		if err := g.Wait(); err != nil {
			return err
		}
		s.log.Debug("updating node store and data-flow", zap.String("node-version", nv.ID),
			zap.String("store", nv.Store), zap.String("name", event.Name))
		start = time.Now()
		err = s.etcd.AddNodeVersionToDataFlowAndIncNodeStore(s.projectID, &nv, delta)
		if err != nil {
			s.log.Warn("etcd not available, fallback to pubsub", zap.Duration("time", time.Now().Sub(start)), zap.String("name", event.Name), zap.Error(err))
			err = s.sendMessageToFallbackEtcdTopic(&nv, delta)
			if err != nil {
				return err
			}
		} else {
			s.log.Info("updated node store and data-flow", zap.String("node-version", nv.ID),
				zap.String("store", nv.Store), zap.Duration("time", time.Now().Sub(start)), zap.String("name", event.Name))
		}
		return nil
	})
	if err != nil {
		s.log.Error("processed file with error", zap.String("name", event.Name),
			zap.String("bucket", event.Bucket), zap.Duration("time", time.Since(mStart)),
			zap.Error(err))
	} else {
		s.log.Info("processed file", zap.String("name", event.Name),
			zap.String("bucket", event.Bucket), zap.Duration("time", time.Since(mStart)))
	}
	return err
}

func (s *VeroStore) addHoldToFile(bucket, name string) error {
	o := s.stClient.Bucket(bucket).Object(name)
	objectAttrsToUpdate := storage.ObjectAttrsToUpdate{
		EventBasedHold: s.versioning,
		TemporaryHold:  true,
	}
	_, err := o.Update(context.Background(), objectAttrsToUpdate)
	return err
}

func (s *VeroStore) sendMessageToFallbackEtcdTopic(nv *model.NodeVersion, delta int) error {
	attrs := make(map[string]string)
	attrs["delta"] = strconv.Itoa(delta)
	data, err := json.Marshal(nv)
	if err != nil {
		return err
	}
	m := &pubsub.Message{
		Data:       data,
		Attributes: attrs,
	}
	r := s.topicFallbackEtcd.Publish(context.Background(), m)
	messageID, err := r.Get(context.Background())
	if err != nil {
		return err
	} else {
		s.log.Info("message published to fallback topic", zap.String("name", nv.NodeID), zap.String("messageId", messageID))
		return nil
	}
}

func (s *VeroStore) sendMessageToIndexTopic(n *model.Node) error {
	index := s.createElasticIndexStruct(n)
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}
	m := &pubsub.Message{
		Data: data,
	}
	r := s.topicIndexing.Publish(context.Background(), m)
	messageID, err := r.Get(context.Background())
	if err != nil {
		return err
	} else {
		s.log.Info("message published to index", zap.String("name", n.Name), zap.String("messageId", messageID))
		return nil
	}
}

func (s *VeroStore) sendMessageToInvoiceLoaderTopic(n *model.Node) error {
	data, err := json.Marshal(n)
	if err != nil {
		return err
	}
	m := &pubsub.Message{
		Data: data,
	}
	r := s.topicInvoice.Publish(context.Background(), m)
	messageID, err := r.Get(context.Background())
	if err != nil {
		return err
	} else {
		s.log.Info("message published to index", zap.String("name", n.Name), zap.String("messageId", messageID))
		return nil
	}
}

func (s *VeroStore) sendMessageToDeleteTopic(bucket, name string) error {
	uri, err := url.Parse(fmt.Sprintf("gs://%s/%s", bucket, name))
	if err != nil {
		return err
	}
	m := &pubsub.Message{
		Data: []byte(uri.String()),
	}
	r := s.topicInvoice.Publish(context.Background(), m)
	messageID, err := r.Get(context.Background())
	if err != nil {
		return err
	} else {
		s.log.Info("message published to delete topic", zap.String("name", name), zap.String("messageId", messageID))
		return nil
	}
}

func (s *VeroStore) createElasticIndexStruct(n *model.Node) *ElasticIndex {
	return &ElasticIndex{
		Index: fmt.Sprintf("%s_node", s.namespaceIndex),
		ID:    n.ID,
		Body: ElasticBody{
			Name:             n.Name,
			Path:             n.Path,
			ContentType:      strings.Split(n.ContentType, ";")[0],
			LastModifiedDate: n.LastModifiedDate,
			Tags:             n.Metadata.Data["tags"],
		},
	}
}

type ElasticBody struct {
	Name             string      `json:"name"`
	Path             string      `json:"path"`
	ContentType      string      `json:"contentType"`
	LastModifiedDate string      `json:"lastModifiedDate"`
	Tags             interface{} `json:"tags,omitempty"`
}

type ElasticIndex struct {
	Index string      `json:"index"`
	ID    string      `json:"id"`
	Body  ElasticBody `json:"body"`
}

// ---
//     const body = {
//        name: document.name,
//        path: document.path,
//        contentType: document.contentType.split(';')[0],
//        lastModifiedDate: document.lastModifiedDate,
//    };
//
//    if (document.metadata && document.metadata.tags) {
//        body.tags = document.metadata.tags;
//    }
//
//    const elastic = Elastic.getClient();
//    const namespace = Elastic.getNamespace();
//    const index = namespace ? `${namespace}_node` : 'node';
//
//    await elastic.index({
//        index,
//        id: document.id,
//        body,
//    });

func (s *VeroStore) addPathToCache(key *datastore.Key) {
	_ = s.cache.Put(s.projectID+"/path/"+key.Name, time.Now().Format(time.RFC3339), s.pathExpiration)
}

func (s *VeroStore) checkPath(key *datastore.Key, tx *datastore.Transaction) (bool, error) {
	exists, err := s.cache.Exists(key.Name)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	// the last step is to check in the datastore itself
	var n model.Node
	err = tx.Get(key, &n)
	if err != nil && err != datastore.ErrNoSuchEntity {
		return false, err
	}
	if err == datastore.ErrNoSuchEntity {
		return false, nil
	}
	s.addPathToCache(key)
	return true, nil
}

func (s *VeroStore) addPathInternally(path string, tx *datastore.Transaction) error {
	if strings.HasSuffix(path, "/") {
		path = filepath.Dir(path)
	}
	// we already are in the transaction context
	if tx != nil {
		return s.addIfPathNotExists(path, tx)
	} else {
		_, err := s.dsClient.RunInTransaction(context.Background(), func(tx *datastore.Transaction) error {
			return s.addIfPathNotExists(path, tx)
		})
		return err
	}
}

type nodeKey struct {
	n *model.Node
	k *datastore.Key
}

func (s *VeroStore) addIfPathNotExists(path string, tx *datastore.Transaction) error {
	key := datastore.NameKey("Node", path, nil)
	exists, err := s.checkPath(key, tx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	// normalize path, we do not allow paths ending with slash
	n := model.Node{
		ID:               path,
		Name:             filepath.Base(path),
		NodeType:         model.PATH_TYPE,
		Path:             filepath.Dir(path),
		CreatedDate:      time.Now().Format(time.RFC3339),
		LastModifiedDate: time.Now().Format(time.RFC3339),
	}
	s.log.Info("adding new path", zap.String("path", path))
	_, err = tx.Put(key, &n)
	if err != nil {
		return err
	}
	s.addPathToCache(key)
	if path == "/" {
		return nil
	} else {
		return s.addPathInternally(filepath.Dir(path), tx)
	}
}

func (s *VeroStore) AddPath(path string) error {
	return s.addPathInternally(path, nil)
}

func Checksum(value string) (string, error) {
	r, err := b64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", r), nil
}

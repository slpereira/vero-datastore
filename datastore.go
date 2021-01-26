package store

import (
	"cloud.google.com/go/datastore"
	"context"
	b64 "encoding/base64"
	"fmt"
	"github.com/slpereira/vero-datastore/model"
	"go.uber.org/zap"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// VeroStore implements the access to the datastore, but include some cache for some objects
type VeroStore struct {
	log            *zap.Logger
	client         *datastore.Client
	etcd           *VeroEtcdClient
	projectID      string
	cache          *Cache
	pathExpiration time.Duration
	topicIndexing  string
	topicInvoice   string
}

func NewVeroStore(projectID, redisAddress, redisPwd string, etcdEndpoints []string, log *zap.Logger) (*VeroStore, error) {
	cl, err := datastore.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, err
	}

	pathExpStr := os.Getenv("PATH_CACHE_TTL")

	pathExpiration, err := time.ParseDuration(pathExpStr)

	if err != nil {
		pathExpiration = 0
	}

	etcd, err := NewVeroEtcdClient(etcdEndpoints, log)
	if err != nil {
		return nil, err
	}

	return &VeroStore{client: cl,
		projectID:      projectID,
		cache:          NewCache(redisAddress, redisPwd),
		pathExpiration: pathExpiration,
		etcd:           etcd,
		log:            log,
	}, nil
}

func (s *VeroStore) GetNode(ID string) (*model.Node, error) {
	s.log.Debug("datastore:node:get", zap.String("id", ID))
	key := datastore.NameKey("Node", ID, nil)
	var n model.Node
	err := s.client.Get(context.Background(), key, &n)
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
	err := s.client.Get(context.Background(), key, &n)
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
	_, err := s.client.Put(context.Background(), key, &n)
	if err != nil {
		return err
	}
	return nil
}

func (s *VeroStore) PutNodeVersion(n *model.NodeVersion) error {
	s.log.Debug("datastore:node-version:put", zap.String("id", n.ID))
	key := datastore.NameKey("NodeVersion", n.ID, nil)
	_, err := s.client.Put(context.Background(), key, n)
	return err
}

func (s *VeroStore) AddFileToVero(ctx context.Context, event model.GCSEvent) error {
	s.log.Info("add file to vero", zap.String("name", event.Name),
		zap.String("bucket", event.Bucket))
	mStart := time.Now()
	// if file is zero size, discard them
	if event.Size == "0" {
		s.log.Warn("zero size file", zap.String("name", event.Name),
			zap.String("bucket", event.Bucket))
		// delete from storage
		return nil
	}
	cs, err := Checksum(event.MD5Hash)
	if err != nil {
		return err
	}
	size, err := strconv.Atoi(event.Size)
	if err != nil {
		return err
	}
	_, err = s.client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		// Node
		nodeID := filepath.Join("/", event.Name)
		nodeKey := datastore.NameKey("Node", nodeID, nil)
		var n model.Node
		var nv model.NodeVersion
		s.log.Debug("searching for node",zap.String("name", event.Name))
		start := time.Now()
		err := tx.Get(nodeKey, &n)
		s.log.Info("node searched", zap.String("name", event.Name), zap.Duration("time", time.Since(start)))
		if err != nil && err != datastore.ErrNoSuchEntity {
			return err
		}
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
			var p datastore.PropertyList
			for k,v := range event.Metadata {
				p = append(p, datastore.Property{
					Name:    k,
					Value:   v,
				})
			}
			n.Metadata = p
			n.Owner = event.Bucket
			s.log.Debug("checking path", zap.String("path", n.Path), zap.String("name", event.Name))
			start = time.Now()
			// add path only if the file is completely new, otherwise the path already exists
			if err = s.addPathInternally(n.Path, tx); err != nil {
				return err
			}
			s.log.Info("checked path", zap.String("path", n.Path), zap.String("name", event.Name), zap.Duration("time", time.Now().Sub(start)))
		} else {
			// same file???
			if cs == n.Checksum {
				s.log.Warn("same checksum", zap.String("name", event.Name), zap.String("bucket", event.Bucket))
				return nil
			}
			// TODO(silvio) if the new file is replacing an existing file in the bucket before the compression execute, it is not a new version
			// the new version happens only if the current file is already compressed in the destination bucket
			// is necessary to have some approach to guarantee isolation for compression process and the current file in the bucket

			// Node exists, we are updating the file, checking versioning and if the file really changed comparing the Checksum
			n.ActiveVersionNumber++
			n.Store = event.Bucket
			n.ContentType = event.ContentType
			n.ContentLength = size
			n.LastModifiedDate = event.Updated.Format(time.RFC3339)
			var p datastore.PropertyList
			for k,v := range event.Metadata {
				p = append(p, datastore.Property{
					Name:    k,
					Value:   v,
				})
			}
			n.Metadata = p
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
		s.log.Info("added node version and node",  zap.String("node-version", nv.ID),
			zap.String("node", n.ID), zap.Duration("time", time.Since(start)), zap.String("name", event.Name))
		if err != nil {
			return err
		}
		s.log.Debug("updating node store and data-flow", zap.String("node-version", nv.ID),
			zap.String("store", nv.Store), zap.String("name", event.Name))
		start = time.Now()
		err = s.etcd.AddNodeVersionAndNodeStore(s.projectID, &nv)
		s.log.Info("updated node store and data-flow", zap.String("node-version", nv.ID),
			zap.String("store", nv.Store), zap.Duration("time", time.Now().Sub(start)), zap.String("name", event.Name))
		if err != nil {
			return err
		}
		// must index the metadata in the elastic
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
		_, err := s.client.RunInTransaction(context.Background(), func(tx *datastore.Transaction) error {
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

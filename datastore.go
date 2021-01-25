package store

import (
	"cloud.google.com/go/datastore"
	"context"
	b64 "encoding/base64"
	"fmt"
	"github.com/slpereira/vero-datastore/model"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// VeroStore implements the access to the datastore, but include some cache for some objects
type VeroStore struct {
	client         *datastore.Client
	etcd           *VeroEtcdClient
	projectID      string
	cache          *Cache
	pathExpiration time.Duration
}

func NewVeroStore(projectID, redisAddress, redisPwd string, etcdEndpoints []string) (*VeroStore, error) {
	cl, err := datastore.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, err
	}

	pathExpStr := os.Getenv("PATH_CACHE_TTL")

	pathExpiration, err := time.ParseDuration(pathExpStr)

	if err != nil {
		log.Printf("invalid path cache ttl param:%v\n", err)
		pathExpiration = 0
	}

	etcd, err := NewVeroEtcdClient(etcdEndpoints)
	if err != nil {
		return nil, err
	}

	return &VeroStore{client: cl,
		projectID:      projectID,
		cache:          NewCache(redisAddress, redisPwd),
		pathExpiration: pathExpiration,
		etcd:           etcd,
	}, nil
}

func (s *VeroStore) GetNode(ID string) (*model.Node, error) {
	log.Printf("datastore:node:get %s\n", ID)
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
	log.Printf("datastore:node-version:get %s\n", ID)
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
	log.Printf("datastore:node:put %s\n", n.ID)
	key := datastore.NameKey("Node", n.ID, nil)
	_, err := s.client.Put(context.Background(), key, &n)
	if err != nil {
		return err
	}
	return nil
}

func (s *VeroStore) PutNodeVersion(n *model.NodeVersion) error {
	log.Printf("datastore:node-version:put %s\n", n.ID)
	key := datastore.NameKey("NodeVersion", n.ID, nil)
	_, err := s.client.Put(context.Background(), key, n)
	return err
}

func (s *VeroStore) AddFileToVero(ctx context.Context, event model.GCSEvent) error {
	log.Printf("processing file %s from %s\n", event.Name, event.Bucket)
	mStart := time.Now()
	// if file is zero size, discard them
	if event.Size == "0" {
		log.Printf("file %s from bucket %s is zero size\n", event.Name, event.Bucket)
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
		nodeID := filepath.Join("/", filepath.Clean(event.Name))
		nodeKey := datastore.NameKey("Node", nodeID, nil)
		var n model.Node
		var nv model.NodeVersion
		log.Printf("looking for node: %s\n", nodeKey.Name)
		start := time.Now()
		err := tx.Get(nodeKey, &n)
		log.Printf("looking for node: %s took %v\n", nodeKey.Name, time.Now().Sub(start))
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
			n.Uri = url.QueryEscape(fmt.Sprintf("gs://%s/%s", event.Bucket, event.Name))
			n.StorageClass = "Standard"
			n.ActiveVersionNumber = 1
			n.Checksum = cs
			n.Metadata = event.Metadata
			n.Owner = event.Bucket
			log.Printf("checking path:%s", n.Path)
			start = time.Now()
			// add path only if the file is completely new, otherwise the path already exists
			if err = s.addPathInternally(n.Path, tx); err != nil {
				return err
			}
			log.Printf("checking path:%s took %s", n.Path, time.Now().Sub(start))
		} else {
			// same file???
			if cs == n.Checksum {
				log.Printf("file %s from bucket %s does not change the content\n", event.Name, event.Bucket)
				return nil
			}
			// Node exists, we are updating the file, checking versioning and if the file really changed comparing the Checksum
			n.ActiveVersionNumber++
			n.Store = event.Bucket
			n.ContentType = event.ContentType
			n.ContentLength = size
			n.LastModifiedDate = event.Updated.Format(time.RFC3339)
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

		log.Printf("adding node version: %s and node: %s\n", nv.ID, n.ID)
		start = time.Now()
		// add NV
		_, err = tx.PutMulti([]*datastore.Key{nodeKey, nvKey}, []interface{}{&n, &nv})
		log.Printf("adding node version: %s and node: %s took %s\n", nv.ID, n.ID, time.Now().Sub(start))
		if err != nil {
			return err
		}
		log.Printf("adding node version: %s and updating node store: %s\n", nv.ID, nv.Store)
		start = time.Now()
		err = s.etcd.AddNodeVersionAndNodeStore(s.projectID, &nv)
		log.Printf("adding node version: %s and updating node store: %s took %s\n", nv.ID, nv.Store, time.Now().Sub(start))
		if err != nil {
			return err
		}
		// must index the metadata in the elastic
		return nil
	})
	log.Printf("processing file %s from %s took %s\n", event.Name, event.Bucket, time.Now().Sub(mStart))
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
	log.Printf("adding new path: %s\n", path)
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

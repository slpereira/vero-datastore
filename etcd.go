package store

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/dgryski/trifles/uuid"
	"github.com/slpereira/vero-datastore/model"
	"go.uber.org/zap"
	"path"
	"time"
)

type VeroEtcdClient struct {
	client *clientv3.Client
	log    *zap.Logger
}

var ErrNodeStoreNotFound = errors.New("node store not found")
var ErrEtcdNotConnected = errors.New("etcd not connected")

func NewVeroEtcdClient(endpoints []string, username, password string, log *zap.Logger) (*VeroEtcdClient, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,       //   []string{"localhost:2379", "localhost:22379", "localhost:32379"},
		DialTimeout:          5 * time.Second, // TODO parametrize
		DialKeepAliveTimeout: 5 * time.Second, // TODO parametrize
		Username:             username,
		Password:             password,
	})
	if err != nil {
		return nil, err
	}

	return &VeroEtcdClient{
		client: cli,
		log:    log,
	}, nil
}

func (e *VeroEtcdClient) Close() error {
	return e.client.Close()
}

func (e *VeroEtcdClient) AddNodeVersion(projectID string, nv *model.NodeVersion) error {
	bytes, err := json.Marshal(nv)
	if err != nil {
		return err
	}
	id := path.Join(projectID, "node-version", nv.ID)
	e.log.Debug("adding key to etcd", zap.String("key", id))
	_, err = e.client.Put(context.Background(), id, string(bytes))
	return err
}

func (e *VeroEtcdClient) AddNodeVersionToDataFlowAndIncNodeStore(projectID string, nv *model.NodeVersion, delta int) error {
	bytes, err := json.Marshal(nv)
	if err != nil {
		return err
	}
	_, err = concurrency.NewSTM(e.client, func(stm concurrency.STM) error {
		id := path.Join(projectID, "node-version", nv.ID)
		e.log.Debug("adding key to etcd", zap.String("key", id))
		stm.Put(id, string(bytes))
		idNs := path.Join(projectID, "node-store", nv.Store)
		_, err := e.addNodeStoreSTM(stm, idNs, true, nv.Store, nv.ContentLength-delta, 0, 0)
		return err
	})
	return err
}

func (e *VeroEtcdClient) AddNodeStore(projectID string, ns *model.NodeStore) error {
	bytes, err := json.Marshal(ns)
	if err != nil {
		return err
	}
	id := path.Join(projectID, "node-store", ns.ID)
	e.log.Debug("adding key to etcd", zap.String("key", id))
	_, err = e.client.Put(context.Background(), id, string(bytes))
	return err
}

func (e *VeroEtcdClient) FindNodeStoreByPath(context context.Context, projectID string, pathValue string) (*model.NodeStore, error) {
	key := path.Join(projectID, "node-store")
	r, err := e.client.Get(context, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range r.Kvs {
		ns, err := DecodeNodeStore(kv.Value)
		if err != nil {
			return nil, err
		}
		if ns.Path == pathValue {
			return ns, nil
		}
	}
	// TODO Check if returning an error indicating not found is more appropriated
	return nil, nil
}

func (e *VeroEtcdClient) FindNodeStore(context context.Context, projectID string, nodeStoreID string) (*model.NodeStore, error) {
	key := path.Join(projectID, "node-store", nodeStoreID)
	r, err := e.client.Get(context, key)
	if err != nil {
		return nil, err
	}
	if r.Count >= 1 {
		return DecodeNodeStore(r.Kvs[0].Value)
	}
	// TODO Check if returning an error indicating not found is more appropriated
	return nil, nil
}

func (e *VeroEtcdClient) SumNodeStore(projectID, bucket string, totalSpace, sizeChunk, sizeBlk int, addIfNotExists bool) (*model.NodeStore, error) {
	var retValue *model.NodeStore
	id := path.Join(projectID, "node-store", bucket)
	_, err := concurrency.NewSTM(e.client, func(stm concurrency.STM) error {
		ns, err := e.addNodeStoreSTM(stm, id, addIfNotExists, bucket, totalSpace, sizeChunk, sizeBlk)
		if err != nil {
			return err
		}
		retValue = ns
		return nil
	})
	if err != nil {
		return nil, err
	}
	return retValue, nil
}

func (e *VeroEtcdClient) addNodeStoreSTM(stm concurrency.STM, id string, addIfNotExists bool, bucket string, totalSpace int, sizeChunk int, sizeBlk int) (*model.NodeStore, error) {
	data := stm.Get(id)
	var ns *model.NodeStore
	if data == "" {
		if !addIfNotExists {
			return nil, ErrNodeStoreNotFound
		}
		ns = &model.NodeStore{
			ID:         id,
			Name:       bucket,
			NodeType:   "gs",
			Path:       "/",
			ReadOnly:   false,
			TotalSpace: totalSpace,
			SizeChunk:  sizeChunk,
			SizeBlk:    sizeBlk,
		}
	} else {
		var err error
		ns, err = DecodeNodeStore([]byte(data))
		if err != nil {
			return nil, err
		}
		ns.SizeChunk += sizeChunk
		ns.TotalSpace += totalSpace
		ns.SizeBlk += sizeBlk
	}
	bytes, err := json.Marshal(ns)
	if err != nil {
		return nil, err
	}
	stm.Put(id, string(bytes))
	e.log.Info("incremented node store", zap.String("store", id), zap.Int("contentLength", totalSpace))
	return ns, nil
}

func (e *VeroEtcdClient) SumNodeStoreAsync(context context.Context, projectID, nodeStoreID string, totalSpace, sizeChunk, sizeBlk int) error {
	ID := uuid.UUIDv4()
	key := path.Join(projectID, "node-store-async", ID)
	data, err := json.Marshal(&model.UpdateNodeStoreAsyncData{
		ID:         nodeStoreID,
		TotalSpace: totalSpace,
		SizeChunk:  sizeChunk,
		SizeBlk:    sizeBlk,
	})
	if err != nil {
		return err
	}
	_, err = e.client.Put(context, key, string(data))
	return err
}

func DecodeNodeStore(v []byte) (*model.NodeStore, error) {
	var ns model.NodeStore
	if err := json.Unmarshal(v, &ns); err != nil {
		return nil, err
	}
	return &ns, nil
}

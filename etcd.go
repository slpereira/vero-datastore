package store

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/slpereira/vero-datastore/model"
	"go.uber.org/zap"
	"path"
	"time"
)

type VeroEtcdClient struct {
	client *clientv3.Client
	log    *zap.Logger
}

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
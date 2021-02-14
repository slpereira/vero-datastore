package store

import (
	"context"
	"github.com/slpereira/vero-datastore/model"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"testing"
)

func TestVeroEtcdClient_SumNodeStore(t *testing.T) {
	t.Run("test-sum-node-non-existent", func(t *testing.T) {
		log := zap.NewExample()
		e, err := NewVeroEtcdClient([]string{"104.197.29.130:2379"}, "", "", log)
		if err != nil {
			t.Fatalf("error getting etcd dsClient: %v", err)
		}
		got, err := e.SumNodeStore("tatic-vero-qa", "/invalid", 100, 100, 100, true)
		if err != nil {
			t.Fatalf("error sum node store data: %v", err)
		}
		log.Info("response", zap.String("name", got.Name),
		zap.String("path", got.Path),
		zap.Int("length", got.TotalSpace))
	})
}

func TestVeroEtcdClient_AddNodeVersionToDataFlowAndIncNodeStore(t *testing.T) {
	t.Run("test-sum-node-existent", func(t *testing.T) {
		log := zap.NewExample()
		e, err := NewVeroEtcdClient([]string{"34.75.147.55:2379"}, "", "", log)
		if err != nil {
			t.Fatalf("error getting etcd dsClient: %v", err)
		}
		nv := &model.NodeVersion{
			ID:            "",
			NodeID:        "",
			ContentType:   "",
			ContentLength: 1000,
			Store:         "/teste",
			CreatedDate:   "",
			Deleted:       false,
			VersionNumber: 0,
			StorageClass:  "",
			Uri:           "",
			Checksum:      "",
			Chunks:        nil,
		}
		wg := new(errgroup.Group)
		for i := 0; i<100; i++ {
			wg.Go(func() error {
				return e.AddNodeVersionToDataFlowAndIncNodeStore("tatic-vero-qa", nv, 0)
			})
		}
		err = wg.Wait()
		if err != nil {
			t.Fatalf("error executing: %v", err)
		}
		store, err := e.FindNodeStore(context.Background(), "tatic-vero-qa", "/teste")
		if err != nil {
			t.Fatalf("error sum node store data: %v", err)
		}
		if store.TotalSpace != 100000 {
			t.Fatalf("test failed")
		}
	})
}
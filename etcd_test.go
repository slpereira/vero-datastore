package store

import (
	"github.com/slpereira/vero-datastore/model"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"testing"
)

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
				return e.AddNodeVersion("tatic-vero-qa", nv)
			})
		}
		err = wg.Wait()
		if err != nil {
			t.Fatalf("error executing: %v", err)
		}
	})
}
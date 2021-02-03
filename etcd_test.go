package store

import (
	"go.uber.org/zap"
	"testing"
)

func TestVeroEtcdClient_SumNodeStore(t *testing.T) {
	t.Run("test-sum-node-non-existent", func(t *testing.T) {
		log := zap.NewExample()
		e, err := NewVeroEtcdClient([]string{"104.197.29.130:2379"}, log)
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

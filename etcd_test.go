package store

import (
	"log"
	"testing"
)

func TestVeroEtcdClient_SumNodeStore(t *testing.T) {
	t.Run("test-sum-node-non-existent", func(t *testing.T) {
		e, err := NewVeroEtcdClient([]string{"104.197.29.130:2379"})
		if err != nil {
			t.Fatalf("error getting etcd client: %v", err)
		}
		got, err := e.SumNodeStore("tatic-vero-qa", "/invalid", 100, 100, 100, true)
		if err != nil {
			t.Fatalf("error sum node store data: %v", err)
		}
		log.Printf("resp => %v", got)
	})
}

package store

import (
	"context"
	"github.com/slpereira/vero-datastore/model"
	"go.uber.org/zap"
	"log"
	"net/url"
	"testing"
	"time"
)

func TestChecksum(t *testing.T) {
	t.Run("test-md5", func(t *testing.T) {
		got, _ := Checksum("qNVG+7DkxIGwj+MXxQu2+w==")
		log.Printf("value: %s", got)
	})
}

func TestEncoding(t *testing.T) {
	t.Run("test-encoding", func(t *testing.T) {
		urlt, _ := url.ParseRequestURI ("gs://teste/uname/utest/silvioção")
		log.Printf("%v\n", urlt)
	})
}


func TestVeroDatastore_AddPath(t *testing.T) {
	t.Run("add-path-complex", func(t *testing.T) {
		log := zap.NewExample()
		ds, err := NewVeroStore("tatic-vero-qa", "34.121.69.225:6379", "", []string{"104.197.29.130:2379"}, log)
		if err != nil {
			log.Fatal(err.Error())
		}
		if err := ds.AddPath("/"); err != nil {
			t.Fatalf("AddPath() error = %v", err)
		}
		if err := ds.AddPath("/teste/teste2/teste3"); err != nil {
			t.Fatalf("AddPath() error = %v", err)
		}
		if err := ds.AddPath("/teste/teste2/teste3/teste4"); err != nil {
			t.Fatalf("AddPath() error = %v", err)
		}
		if err := ds.AddPath("/teste-x/teste-x2/teste-x3/teste4"); err != nil {
			t.Fatalf("AddPath() error = %v", err)
		}
		if err := ds.AddPath("/teste-x/teste-x2/teste-x3/teste4/"); err != nil {
			t.Fatalf("AddPath() error = %v", err)
		}
	})
}

func TestVeroStore_AddFileToVero(t *testing.T) {
	t.Run("test add file", func(t *testing.T) {
		log := zap.NewExample()
		ds, err := NewVeroStore("tatic-vero-qa", "34.121.69.225:6379", "", []string{"104.197.29.130:2379"}, log)
		if err != nil {
			log.Fatal(err.Error())
		}
		m := make(map[string]interface{})
		m["tag"] = "tag1,tag2"
		ev := model.GCSEvent{
			Name:           "vup19.db",
			Bucket:         "tatic-vero-in",
			ContentType:    "text/plain",
			TimeCreated:    time.Now(),
			Updated:        time.Now(),
			TemporaryHold:  false,
			EventBasedHold: false,
			StorageClass:   "standard",
			Size:           "10240",
			MD5Hash:        "qNVG+7DkxIGwj+MXxQu2+w==",
			Metadata:       m,
		}
		if err := ds.AddFileToVero(context.Background(), ev); err != nil {
			t.Errorf("AddFileToVero() error = %v", err)
		}

		ev = model.GCSEvent{
			Name:           "../../vup4.db",
			Bucket:         "tatic-vero-in",
			ContentType:    "text/plain",
			TimeCreated:    time.Now(),
			Updated:        time.Now(),
			TemporaryHold:  false,
			EventBasedHold: false,
			StorageClass:   "standard",
			Size:           "10240",
			MD5Hash:        "qNVG+7DkxIGwj+MXxQu2+w==",
			Metadata:       nil,
		}
		if err := ds.AddFileToVero(context.Background(), ev); err != nil {
			t.Errorf("AddFileToVero() error = %v", err)
		}

	})
}

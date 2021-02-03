package model

import "testing"

func TestMetadata_HasKey(t *testing.T) {
	t.Run("has no key and no panic", func(t *testing.T) {
		m := NewMetadata(nil)
		got := m.HasKey("unknown")
		if got {
			t.Fatal("test failed")
		}
	})

	t.Run("has key", func(t *testing.T) {
		meta := make(map[string]interface{})
		meta["unknown"] = "test"
		m := NewMetadata(meta)
		got := m.HasKey("unknown")
		if !got {
			t.Fatal("test failed")
		}
	})

}

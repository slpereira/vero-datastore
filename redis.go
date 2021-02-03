package store

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type Cache struct {
	client                  *redis.ClusterClient
	allowNotConnectedOption bool
}

func NewCache(addrs []string, password string, allowNotConnectedOption bool) *Cache {
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Password: password, // no password set
	})
	return &Cache{
		client:                  rdb,
		allowNotConnectedOption: allowNotConnectedOption,
	}
}

func (c *Cache) Exists(key string) (bool, error) {
	r, err := c.client.Exists(context.Background(), key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		if c.allowNotConnectedOption {
			return false, nil
		}
		return false, err
	}
	return r > 0, err
}

func (c *Cache) Put(key, value string, expiration time.Duration) error {
	err := c.client.Set(context.Background(), key, value, expiration).Err()
	if err != nil && c.allowNotConnectedOption {
		return nil
	}
	return err
}

func (c *Cache) Remove(key string) error {
	err := c.client.Del(context.Background(), key).Err()
	if err != nil && c.allowNotConnectedOption {
		return nil
	}
	return err
}

func (c *Cache) Close() error {
	return c.client.Close()
}

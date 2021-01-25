package store

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type Cache struct {
	client *redis.Client
}

func NewCache(address, password string) *Cache {
	rdb := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password, // no password set
		DB:       0,        // use default DB
	})
	return &Cache{rdb}
}

func (c *Cache) Exists(key string) (bool, error) {
	r, err := c.client.Exists(context.Background(), key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return r > 0, err
}

func (c *Cache) Put(key, value string, expiration time.Duration) error {
	return c.client.Set(context.Background(), key, value, expiration).Err()
}

func (c *Cache) Remove(key string) error {
	return c.client.Del(context.Background(), key).Err()
}

func (c *Cache) Close() error {
	return c.client.Close()
}

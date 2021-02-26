package store

import (
	"context"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"time"
)

type RedisClusterCache struct {
	clusterClient           *redis.ClusterClient
	allowNotConnectedOption bool
	logger                  *zap.Logger
}

type RedisCache struct {
	client                  *redis.Client
	allowNotConnectedOption bool
	logger                  *zap.Logger
}

type MemCache interface {
	Exists(key string) (bool, error)
	Put(key, value string, expiration time.Duration) error
	Remove(key string) error
	Close() error
}

func NewMemCache(addrs []string, password string,
	allowNotConnectedOption bool, logger *zap.Logger, clustered bool) MemCache {
	if clustered {
		rdb := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    addrs,
			Password: password, // no password set
		})
		return &RedisClusterCache{
			clusterClient:           rdb,
			allowNotConnectedOption: allowNotConnectedOption,
			logger:                  logger,
		}
	} else {
		rdb := redis.NewClient(&redis.Options{
			Addr:     addrs[0],
			Password: password, // no password set
		})
		return &RedisCache{
			client:                  rdb,
			allowNotConnectedOption: allowNotConnectedOption,
			logger:                  logger,
		}
	}
}

func (r RedisCache) Exists(key string) (bool, error) {
	c, err := r.client.Exists(context.Background(), key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		if r.allowNotConnectedOption {
			r.logger.Warn("redis error", zap.Error(err))
			return false, nil
		}
		return false, err
	}
	return c > 0, err
}

func (r RedisCache) Put(key, value string, expiration time.Duration) error {
	err := r.client.Set(context.Background(), key, value, expiration).Err()
	if err != nil && r.allowNotConnectedOption {
		return nil
	}
	return err

}

func (r RedisCache) Remove(key string) error {
	err := r.client.Del(context.Background(), key).Err()
	if err != nil && r.allowNotConnectedOption {
		return nil
	}
	return err
}

func (r RedisCache) Close() error {
	return r.client.Close()
}

func (c *RedisClusterCache) Exists(key string) (bool, error) {
	r, err := c.clusterClient.Exists(context.Background(), key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		if c.allowNotConnectedOption {
			c.logger.Warn("redis error", zap.Error(err))
			return false, nil
		}
		return false, err
	}
	return r > 0, err
}

func (c *RedisClusterCache) Put(key, value string, expiration time.Duration) error {
	err := c.clusterClient.Set(context.Background(), key, value, expiration).Err()
	if err != nil && c.allowNotConnectedOption {
		return nil
	}
	return err
}

func (c *RedisClusterCache) Remove(key string) error {
	err := c.clusterClient.Del(context.Background(), key).Err()
	if err != nil && c.allowNotConnectedOption {
		return nil
	}
	return err
}

func (c *RedisClusterCache) Close() error {
	return c.clusterClient.Close()
}

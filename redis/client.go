package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goRedis "github.com/redis/go-redis/v9"
)

type Client interface {
	GetData(ctx context.Context, key string) (string, error)
	HGetAllData(ctx context.Context, key string) (map[string]string, error)
	HGetData(ctx context.Context, key, field string) (string, error)
	SetData(ctx context.Context, key string, value any, ttl ...time.Duration) error
	SetNX(ctx context.Context, key string, value any, ttl ...time.Duration) (bool, error)
	HSetData(ctx context.Context, key string, value ...any) error
	Exists(ctx context.Context, key ...string) (bool, error)
	Delete(ctx context.Context, key ...string) error
	HDelete(ctx context.Context, key string, value ...string) error
	Incr(ctx context.Context, key string) (int64, error)
	Decr(ctx context.Context, key string) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	TTL(ctx context.Context, key string) (time.Duration, error)
	Scan(ctx context.Context, pattern string, count int64) ([]string, error)
	Lock(ctx context.Context, name, secret string, ttl ...time.Duration) (bool, error)
	Unlock(ctx context.Context, name, secret string) (bool, error)
	RedisClient() *goRedis.Client
	WithRetryableDistributedLock(ctx context.Context, key string, fn func() (any, error), timeout, retryPeriod time.Duration, ttl ...time.Duration) (any, error)
	WithDistributedLock(ctx context.Context, key string, fn func() (any, error), ttl ...time.Duration) (any, error)
}

func New(host, password, port string) (Client, error) {
	c := goRedis.NewClient(&goRedis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Password: password,
		DB:       0,
	})
	return &redisClient{
		client: c,
	}, nil
}

type redisClient struct {
	client *goRedis.Client
}

func (c *redisClient) GetData(ctx context.Context, key string) (string, error) {
	data, err := c.client.Get(ctx, key).Result()
	if err != nil {
		return "", fmt.Errorf("error get key %s: %w", key, err)
	}
	return data, nil
}

func (c *redisClient) HGetAllData(ctx context.Context, key string) (map[string]string, error) {
	data, err := c.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("error hget all key %s: %w", key, err)
	}
	return data, nil
}

func (c *redisClient) HGetData(ctx context.Context, key, field string) (string, error) {
	data, err := c.client.HGet(ctx, key, field).Result()
	if err != nil {
		return "", fmt.Errorf("error hget key %s: %w", key, err)
	}
	return data, nil
}

func (c *redisClient) SetData(ctx context.Context, key string, value any, ttl ...time.Duration) error {
	expiration := time.Duration(0)
	if len(ttl) > 0 {
		expiration = ttl[0]
	}
	if err := c.client.Set(ctx, key, value, expiration).Err(); err != nil {
		return fmt.Errorf("error set %s: %w", key, err)
	}
	return nil
}

func (c *redisClient) SetNX(ctx context.Context, key string, value any, ttl ...time.Duration) (bool, error) {
	expiration := time.Duration(0)
	if len(ttl) > 0 {
		expiration = ttl[0]
	}
	ok, err := c.client.SetNX(ctx, key, value, expiration).Result()
	if err != nil {
		return false, fmt.Errorf("error setnx %s: %w", key, err)
	}
	return ok, nil
}

func (c *redisClient) HSetData(ctx context.Context, key string, value ...any) error {
	if len(value) < 2 {
		return errors.New("hset require group key")
	}
	params := value[0:2]
	var ttl time.Duration
	if len(value) == 3 {
		var ok bool
		ttl, ok = value[2].(time.Duration)
		if !ok {
			return errors.New("wrong format ttl")
		}
	}
	if err := c.client.HSet(ctx, key, params).Err(); err != nil {
		return fmt.Errorf("error hset %s: %w", key, err)
	}
	if ttl > 0 {
		if err := c.client.Expire(ctx, key, ttl).Err(); err != nil {
			return fmt.Errorf("error hset expire %s: %w", key, err)
		}
	}
	return nil
}

func (c *redisClient) Exists(ctx context.Context, key ...string) (bool, error) {
	count, err := c.client.Exists(ctx, key...).Result()
	if err != nil {
		return false, fmt.Errorf("error exists %v: %w", key, err)
	}
	return count > 0, nil
}

func (c *redisClient) Delete(ctx context.Context, key ...string) error {
	if err := c.client.Del(ctx, key...).Err(); err != nil {
		return fmt.Errorf("error delete %v: %w", key, err)
	}
	return nil
}

func (c *redisClient) HDelete(ctx context.Context, key string, fields ...string) error {
	if err := c.client.HDel(ctx, key, fields...).Err(); err != nil {
		return fmt.Errorf("error hdel %v: %w", key, err)
	}
	return nil
}

func (c *redisClient) Incr(ctx context.Context, key string) (int64, error) {
	val, err := c.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("error incr %s: %w", key, err)
	}
	return val, nil
}

func (c *redisClient) Decr(ctx context.Context, key string) (int64, error) {
	val, err := c.client.Decr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("error decr %s: %w", key, err)
	}
	return val, nil
}

func (c *redisClient) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := c.client.Expire(ctx, key, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("error expire %s: %w", key, err)
	}
	return ok, nil
}

func (c *redisClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	d, err := c.client.TTL(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("error ttl %s: %w", key, err)
	}
	return d, nil
}

func (c *redisClient) Scan(ctx context.Context, pattern string, count int64) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		batch, next, err := c.client.Scan(ctx, cursor, pattern, count).Result()
		if err != nil {
			return nil, fmt.Errorf("error scan %s: %w", pattern, err)
		}
		keys = append(keys, batch...)
		if next == 0 {
			break
		}
		cursor = next
	}
	return keys, nil
}


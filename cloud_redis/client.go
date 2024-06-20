package cloud_redis

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	goRedis "github.com/redis/go-redis/v9"
)

type Redis interface {
	GetData(ctx context.Context, key string) (data string, err error)
	HGetAllData(ctx context.Context, key string) (map[string]string, error)
	HGetData(ctx context.Context, key, field string) (string, error)
	SetData(ctx context.Context, key string, value interface{}, ttl ...time.Duration) (err error)
	HSetData(ctx context.Context, key string, value ...interface{}) (err error)
	Exists(ctx context.Context, key ...string) (bool, error)
	Delete(ctx context.Context, key ...string) error
	HDelete(ctx context.Context, key string, value ...string) error
	Lock(ctx context.Context, name, secret string, ttl ...time.Duration) (bool, error)
	Unlock(ctx context.Context, name, secret string) (bool, error)
	Client() *goRedis.Client
}

func New(host, password string, port string, ttl time.Duration) (Redis, error) {
	client := goRedis.NewClient(&goRedis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Password: password,
		DB:       0,
	})
	cleanUpCloudClient(client)
	return &CloudRedis{
		client: client,
		ttl:    ttl,
	}, nil
}

type CloudRedis struct {
	client *goRedis.Client
	ttl    time.Duration
}

func (c *CloudRedis) GetData(ctx context.Context, key string) (data string, err error) {
	response := c.client.Get(ctx, key)
	if response.Err() != nil {
		return "", fmt.Errorf("error get key %s: %v", key, response.Err())
	} else if data, err := response.Result(); err != nil {
		return "", fmt.Errorf("error get key %s: %v", key, err)
	} else {
		return data, nil
	}
}

func (c *CloudRedis) HGetAllData(ctx context.Context, key string) (map[string]string, error) {
	response := c.client.HGetAll(ctx, key)
	if response.Err() != nil {
		return nil, fmt.Errorf("error hget all key %s: %v", key, response.Err())
	} else if data, err := response.Result(); err != nil {
		return nil, fmt.Errorf("error hget all key %s: %v", key, err)
	} else {
		return data, nil
	}
}

func (c *CloudRedis) HGetData(ctx context.Context, key, field string) (string, error) {
	response := c.client.HGet(ctx, key, field)
	if response.Err() != nil {
		return "", fmt.Errorf("error hget key %s: %v", key, response.Err())
	} else if data, err := response.Result(); err != nil {
		return "", fmt.Errorf("error hget key %s: %v", key, err)
	} else {
		return data, nil
	}
}

func (c *CloudRedis) SetData(ctx context.Context, key string, value interface{}, ttl ...time.Duration) (err error) {
	var response *goRedis.StatusCmd
	if len(ttl) > 0 {
		response = c.client.Set(ctx, key, value, ttl[0])
	} else {
		response = c.client.Set(ctx, key, value, 0)
	}
	if response.Err() != nil {
		return fmt.Errorf("error set %s: %v", key, response.Err())
	}
	return nil
}

func (c *CloudRedis) HSetData(ctx context.Context, key string, value ...interface{}) (err error) {
	response := c.client.HSet(ctx, key, value)
	if response.Err() != nil {
		return fmt.Errorf("error hset %s: %v", key, response.Err())
	}
	if c.ttl.Milliseconds() > 0 {
		duration := c.client.Expire(ctx, key, c.ttl)
		if duration.Err() != nil {
			return fmt.Errorf("error host %s: %v", key, duration.Err())
		}
	} else {
		duration := c.client.Expire(ctx, key, 0)
		if duration.Err() != nil {
			return fmt.Errorf("error host %s: %v", key, duration.Err())
		}
	}
	return nil
}

func (c *CloudRedis) Exists(ctx context.Context, key ...string) (bool, error) {
	response := c.client.Exists(ctx, key...)
	if response.Err() != nil {
		return false, fmt.Errorf("error exists %v: %v", key, response.Err())
	} else if data, err := response.Result(); err != nil {
		return false, fmt.Errorf("error exists %v: %v", key, response.Err())
	} else {
		return data > 0, nil
	}
}

func (c *CloudRedis) Delete(ctx context.Context, key ...string) error {
	response := c.client.Del(ctx, key...)
	if response.Err() != nil {
		return fmt.Errorf("error delete %v: %v", key, response.Err())
	} else {
		return nil
	}
}

func (c *CloudRedis) HDelete(ctx context.Context, key string, fields ...string) error {
	response := c.client.HDel(ctx, key, fields...)
	if response.Err() != nil {
		return fmt.Errorf("error delete %v: %v", key, response.Err())
	} else {
		return nil
	}
}

func cleanUpCloudClient(pool *goRedis.Client) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		_ = pool.Close()
		os.Exit(0)
	}()
}

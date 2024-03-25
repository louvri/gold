package cloud_redis

import (
	"context"
	"fmt"
	"time"

	goRedis "github.com/redis/go-redis/v9"
)

const (
	SessionLockScript = `
		local v = redis.call("GET", KEYS[1])
		if v == false or v == ARGV[1]
		then
			return redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2]) and 1
		else
			return 0
		end`

	SessionUnlockScript = `
		local v = redis.call("GET",KEYS[1])
		if v == false then
			return 1
		elseif v == ARGV[1] then
		return redis.call("DEL",KEYS[1])
		else
			return 0
		end`
)

func (c *CloudRedis) Lock(ctx context.Context, name, secret string, ttl ...time.Duration) (bool, error) {
	script := goRedis.NewScript(SessionLockScript)
	resp, err := script.Run(ctx, c.client, []string{name}, []string{secret}).Int()
	if err != nil {
		return false, err
	}
	if resp == 0 {
		return false, nil
	}
	if len(ttl) > 0 {
		duration := c.client.Expire(ctx, name, ttl[0])
		if duration.Err() != nil {
			return false, fmt.Errorf("error host %s: %v", name, duration.Err())
		}
	} else {
		duration := c.client.Expire(ctx, name, 0)
		if duration.Err() != nil {
			return false, fmt.Errorf("error host %s: %v", name, duration.Err())
		}
	}
	return true, nil
}

func (c *CloudRedis) Unlock(ctx context.Context, name, secret string) (bool, error) {
	script := goRedis.NewScript(SessionUnlockScript)
	resp, err := script.Run(ctx, c.client, []string{name}, []string{secret}).Int()
	if err != nil {
		return false, err
	}
	if resp == 0 {
		return false, nil
	}
	return true, nil
}

func (c *CloudRedis) Client() *goRedis.Client {
	return c.client
}

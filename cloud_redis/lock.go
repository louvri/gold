package cloud_redis

import (
	"context"
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
	var response int
	var err error
	script := goRedis.NewScript(SessionLockScript)
	if len(ttl) > 0 {
		response, err = script.Run(ctx, c.client, []string{name}, secret, ttl[0]).Int()
	} else {
		response, err = script.Run(ctx, c.client, []string{name}, secret, 24*time.Hour).Int()
	}

	if err != nil {
		return false, err
	}
	if response == 0 {
		return false, nil
	}
	return true, nil
}

func (c *CloudRedis) Unlock(ctx context.Context, name, secret string) (bool, error) {
	script := goRedis.NewScript(SessionUnlockScript)
	resp, err := script.Run(ctx, c.client, []string{name}, secret).Int()
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

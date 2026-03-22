package redis

import (
	"context"
	"fmt"
	"strconv"
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

func (c *redisClient) Lock(ctx context.Context, name, secret string, ttl ...time.Duration) (bool, error) {
	lockTTL := 24 * time.Hour
	if len(ttl) > 0 {
		lockTTL = ttl[0]
	}
	script := goRedis.NewScript(SessionLockScript)
	result := script.Run(ctx, c.client, []string{name}, secret, int(lockTTL.Seconds()))
	if err := result.Err(); err != nil {
		return false, fmt.Errorf("lock %s: %w", name, err)
	}
	response, err := toInt(result.Val())
	if err != nil {
		return false, err
	}
	return response != 0, nil
}

func (c *redisClient) Unlock(ctx context.Context, name, secret string) (bool, error) {
	script := goRedis.NewScript(SessionUnlockScript)
	result := script.Run(ctx, c.client, []string{name}, secret)
	if err := result.Err(); err != nil {
		return false, fmt.Errorf("unlock %s: %w", name, err)
	}
	response, err := toInt(result.Val())
	if err != nil {
		return false, err
	}
	return response != 0, nil
}

func (c *redisClient) RedisClient() *goRedis.Client {
	return c.client
}

func toInt(v any) (int, error) {
	switch val := v.(type) {
	case int:
		return val, nil
	case int64:
		return int(val), nil
	case float64:
		return int(val), nil
	case string:
		return strconv.Atoi(val)
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

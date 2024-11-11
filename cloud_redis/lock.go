package cloud_redis

import (
	"context"
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

func (c *CloudRedis) Lock(ctx context.Context, name, secret string, ttl ...time.Duration) (bool, error) {
	var tmp interface{}
	var response int
	var err error
	var ok bool
	script := goRedis.NewScript(SessionLockScript)
	if len(ttl) > 0 {
		tmp = script.Run(ctx, c.client, []string{name}, secret, ttl[0]).Val()

	} else {
		tmp = script.Run(ctx, c.client, []string{name}, secret, 24*time.Hour).Val()
	}

	if response, ok = tmp.(int); !ok {
		if integer64, ok := tmp.(int64); !ok {
			if floater64, ok := tmp.(float64); !ok {
				if str, ok := tmp.(string); ok {
					response, err = strconv.Atoi(str)
					if err != nil {
						return false, err
					}
				}
			} else {
				response = int(floater64)
			}

		} else {
			response = int(integer64)
		}

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
	var tmp interface{}
	var response int
	var err error
	var ok bool

	script := goRedis.NewScript(SessionUnlockScript)
	tmp = script.Run(ctx, c.client, []string{name}, secret).Val()

	if response, ok = tmp.(int); !ok {
		if integer64, ok := tmp.(int64); !ok {
			if floater64, ok := tmp.(float64); !ok {
				if str, ok := tmp.(string); ok {
					response, err = strconv.Atoi(str)
					if err != nil {
						return false, err
					}
				}
			} else {
				response = int(floater64)
			}

		} else {
			response = int(integer64)
		}

	}

	if response == 0 {
		return false, nil
	}
	return true, nil
}

func (c *CloudRedis) Client() *goRedis.Client {
	return c.client
}

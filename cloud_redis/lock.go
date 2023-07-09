package cloud_redis

import (
	"github.com/gomodule/redigo/redis"
	"strings"
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

func (cr *CloudRedis) Lock(name, secret string, ttl ...int64) (bool, error) {
	conn := cr.Pool.Get()
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	timeToLive := int64(cr.ttl)
	if len(ttl) > 0 {
		timeToLive = ttl[0]
	}
	script := redis.NewScript(1, SessionLockScript)
	resp, err := redis.Int(script.Do(conn, name, secret, timeToLive))
	if err != nil {
		return false, err
	}
	if resp == 0 {
		return false, nil
	}
	return true, nil
}

func (cr *CloudRedis) Unlock(name, secret string) (bool, error) {
	conn := cr.Pool.Get()
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	script := redis.NewScript(1, SessionUnlockScript)
	resp, err := redis.Int(script.Do(conn, name, secret))
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "closed pool") {
			conn, err = cr.SingleDial()
			defer func(conn redis.Conn) {
				_ = conn.Close()
			}(conn)
			if err != nil {
				return false, err
			}
			resp, err = redis.Int(script.Do(conn, name, secret))
			if err != nil {
				return false, err
			}
		} else {
			return false, err
		}
	}
	if resp == 0 {
		return false, nil
	}
	return true, nil
}

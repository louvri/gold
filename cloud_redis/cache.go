package cloud_redis

import (
	"fmt"

	"github.com/gomodule/redigo/redis"
)

func (cr *CloudRedis) GetData(key string) (data []byte, err error) {
	conn := cr.Pool.Get()
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	data, err = redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return data, fmt.Errorf("error getting key %s: %v", key, err)
	}
	return data, nil
}

func (cr *CloudRedis) HGetAllData(hash string) ([]interface{}, error) {
	conn := cr.Pool.Get()
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)
	if tmp, err := redis.Values(conn.Do("HGETALL", hash)); err != nil {
		return nil, fmt.Errorf("error get all key %s: %v", hash, err)
	} else {
		results := make([]interface{}, 0)
		for i, item := range tmp {
			if (i+1)%2 == 0 {
				results = append(results, item)
			}
		}
		return results, nil
	}
}
func (cr *CloudRedis) HGetData(hash, field string) ([]byte, error) {
	conn := cr.Pool.Get()
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)
	if tmp, err := redis.Bytes(conn.Do("HGET", hash, field)); err != nil {
		return nil, fmt.Errorf("error get all key %s: %v", hash, err)
	} else {
		return tmp, nil
	}
}

func (cr *CloudRedis) SetData(key string, value []byte, ttl ...int64) (err error) {
	conn := cr.Pool.Get()
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	timeToLive := int64(cr.ttl)
	if len(ttl) > 0 {
		timeToLive = ttl[0]
	}

	_, err = conn.Do("SET", key, value)
	if err != nil {
		v := string(value)
		if len(v) > 15 {
			v = v[0:12] + "..."
		}
		return fmt.Errorf("error setting key %s to %s: %v", key, v, err)
	}
	_, err = conn.Do("EXPIRE", key, timeToLive)
	if err != nil {
		return fmt.Errorf("error setting expiry time for key %s to %v: %v", key, timeToLive, err)
	}
	return nil
}

func (cr *CloudRedis) HSetData(hash, field string, data []byte, ttl ...int64) error {
	conn := cr.Pool.Get()
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)
	if _, err := conn.Do("HSet", hash, field, data); err != nil {
		return fmt.Errorf("error hset col : key %s : %s: %v : %v", hash, field, data, err)
	}
	timeToLive := int64(cr.ttl)
	if len(ttl) > 0 {
		timeToLive = ttl[0]
	}

	if _, err := conn.Do("EXPIRE", hash, timeToLive); err != nil {
		return fmt.Errorf("error hset col : key %s : %s: %v : %v", hash, field, data, err)
	}
	return nil
}

func (cr *CloudRedis) Exists(key string) (bool, error) {
	conn := cr.Pool.Get()
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	ok, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return ok, fmt.Errorf("error checking if key %s exists: %v", key, err)
	}
	return ok, err
}

func (cr *CloudRedis) Delete(key string) error {
	conn := cr.Pool.Get()
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	_, err := conn.Do("DEL", key)
	return err
}

func (cr *CloudRedis) HDelete(hash, field string) error {
	conn := cr.Pool.Get()
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)
	_, err := conn.Do("HDEL", hash, field)
	return err
}

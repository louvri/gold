package cloud_redis

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gomodule/redigo/redis"
)

type CloudRedis struct {
	Address  string
	Password string
	Pool     *redis.Pool
	ttl      uint64
}

func (cr *CloudRedis) SingleDial() (result redis.Conn, err error) {
	if cr.Password != "" {
		return redis.Dial("tcp", cr.Address, redis.DialPassword(cr.Password))
	} else {
		return redis.Dial("tcp", cr.Address)
	}
}

func New(host, password string, port string, ttl uint64) CloudRedis {
	address := fmt.Sprintf("%s:%s", host, port)
	pool := initiatePool(address, password)
	cleanUpCloudClient(pool)
	return CloudRedis{
		Address:  address,
		Password: password,
		Pool:     pool,
		ttl:      ttl,
	}
}

func initiatePool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,

		Dial: func() (redis.Conn, error) {
			var err error
			var conn redis.Conn
			if password != "" {
				conn, err = redis.Dial("tcp", server, redis.DialPassword(password))
			} else {
				conn, err = redis.Dial("tcp", server)
			}
			if err != nil {
				return nil, err
			}
			return conn, err
		},

		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func cleanUpCloudClient(pool *redis.Pool) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		_ = pool.Close()
		os.Exit(0)
	}()
}

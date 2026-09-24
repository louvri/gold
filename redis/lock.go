package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	goRedis "github.com/redis/go-redis/v9"
)

const (
	// SessionLockScript acquires KEYS[1] for the owner secret ARGV[1], or
	// renews it when ARGV[1] already owns it, with a TTL of ARGV[2] seconds.
	// It returns 1 on success and 0 when another owner holds the lock.
	SessionLockScript = `
		local v = redis.call("GET", KEYS[1])
		if v == false or v == ARGV[1]
		then
			return redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2]) and 1
		else
			return 0
		end`

	// SessionUnlockScript releases KEYS[1] when ARGV[1] owns it. It returns 1
	// when the lock was released or was already free, and 0 when another owner
	// holds it.
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

// ErrEmptyLockSecret is returned by Lock for an empty secret. The secret
// identifies the lock's owner, so an empty one would make every caller that
// also passes "" its owner, and the lock would exclude no one. Unlock still
// accepts "": it can release only a lock stored with "", such as one an older
// release took, and never another holder's.
var ErrEmptyLockSecret = errors.New("lock secret must not be empty")

// ErrInvalidLockTTL is returned for a non-positive lock TTL.
var ErrInvalidLockTTL = errors.New("lock ttl must be positive")

// ErrInvalidLockRetry is returned by WithRetryableDistributedLock for a
// non-positive timeout or retry period.
var ErrInvalidLockRetry = errors.New("lock retry timeout and period must be positive")

// The scripts are built once: NewScript hashes the source for EVALSHA.
var (
	sessionLockScript   = goRedis.NewScript(SessionLockScript)
	sessionUnlockScript = goRedis.NewScript(SessionUnlockScript)
)

// lockTTL returns the first of ttl, or fallback when none is given, and
// refuses a non-positive TTL: Redis would store the lock without expiry or
// reject it, and a lock without expiry outlives a crashed holder forever.
func lockTTL(name string, fallback time.Duration, ttl []time.Duration) (time.Duration, error) {
	d := fallback
	if len(ttl) > 0 {
		d = ttl[0]
	}
	if d <= 0 {
		return 0, fmt.Errorf("%w: %s got %s", ErrInvalidLockTTL, name, d)
	}
	return d, nil
}

func (c *redisClient) Lock(ctx context.Context, name, secret string, ttl ...time.Duration) (bool, error) {
	if secret == "" {
		return false, fmt.Errorf("%w: %s", ErrEmptyLockSecret, name)
	}
	d, err := lockTTL(name, 24*time.Hour, ttl)
	if err != nil {
		return false, err
	}
	// The script takes whole seconds; round up so a sub-second TTL does not
	// truncate to zero, which Redis rejects.
	result := sessionLockScript.Run(ctx, c.client, []string{name}, secret, int(math.Ceil(d.Seconds())))
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
	result := sessionUnlockScript.Run(ctx, c.client, []string{name}, secret)
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

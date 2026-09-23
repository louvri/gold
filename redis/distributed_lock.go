package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	goRedis "github.com/redis/go-redis/v9"
)

var (
	// ErrDistributedLockNotAcquired is returned when another holder has the
	// lock. WithRetryableDistributedLock returns it when its timeout passes
	// between attempts; a deadline that expires during an attempt surfaces as
	// that attempt's wrapped context error instead.
	ErrDistributedLockNotAcquired = errors.New("distributed lock not acquired")
	// ErrDistributedLockNotHeld reports that a release found the lock no
	// longer held by this client, typically because its TTL expired before fn
	// returned. WithDistributedLock and WithRetryableDistributedLock discard
	// release errors, so it does not reach their callers.
	ErrDistributedLockNotHeld = errors.New("distributed lock not held by this client")
)

// WithRetryableDistributedLock executes a function while holding a distributed lock with retry mechanism.
func (c *redisClient) WithRetryableDistributedLock(ctx context.Context, key string, fn func() (any, error), timeout, retryPeriod time.Duration, ttl ...time.Duration) (any, error) {
	// time.NewTicker panics on a non-positive period.
	if retryPeriod <= 0 {
		return nil, fmt.Errorf("lock %s: retry period must be positive, got %s", key, retryPeriod)
	}
	lockKey := "lock:" + key
	d, err := lockTTL(key, 5*time.Second, ttl)
	if err != nil {
		return nil, err
	}
	lockValue, err := generateUniqueValue()
	if err != nil {
		return nil, err
	}

	err = c.acquireLockWithRetries(ctx, lockKey, lockValue, d, timeout, retryPeriod)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = c.releaseDistributedLock(context.Background(), lockKey, lockValue)
	}()

	return fn()
}

// WithDistributedLock executes a function while holding a distributed lock.
func (c *redisClient) WithDistributedLock(ctx context.Context, key string, fn func() (any, error), ttl ...time.Duration) (any, error) {
	lockKey := "lock:" + key
	d, err := lockTTL(key, 5*time.Second, ttl)
	if err != nil {
		return nil, err
	}
	lockValue, err := generateUniqueValue()
	if err != nil {
		return nil, err
	}

	err = c.acquireDistributedLock(ctx, lockKey, lockValue, d)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = c.releaseDistributedLock(context.Background(), lockKey, lockValue)
	}()

	return fn()
}

// acquireLockWithRetries attempts to acquire the lock with retry mechanism.
func (c *redisClient) acquireLockWithRetries(ctx context.Context, key, value string, ttl, timeout, retryPeriod time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := c.acquireDistributedLock(ctx, key, value, ttl); !errors.Is(err, ErrDistributedLockNotAcquired) {
		return err
	}

	ticker := time.NewTicker(retryPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ErrDistributedLockNotAcquired
		case <-ticker.C:
			if err := c.acquireDistributedLock(ctx, key, value, ttl); !errors.Is(err, ErrDistributedLockNotAcquired) {
				return err
			}
		}
	}
}

// acquireDistributedLock attempts to acquire the distributed lock immediately (fail-fast).
func (c *redisClient) acquireDistributedLock(ctx context.Context, key, value string, ttl time.Duration) error {
	result, err := c.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return fmt.Errorf("acquireDistributedLock %s: %w", key, err)
	}
	if !result {
		return ErrDistributedLockNotAcquired
	}
	return nil
}

// releaseDistributedLock releases the distributed lock.
func (c *redisClient) releaseDistributedLock(ctx context.Context, key, value string) error {
	result, err := distributedUnlockScript.Run(ctx, c.client, []string{key}, value).Result()
	if err != nil {
		return fmt.Errorf("releaseDistributedLock %s: %w", key, err)
	}

	if val, ok := result.(int64); ok && val == 0 {
		return ErrDistributedLockNotHeld
	}

	return nil
}

// distributedUnlockScript deletes KEYS[1] only while it still holds this
// holder's token ARGV[1]. It is built once: NewScript hashes the source for
// EVALSHA.
var distributedUnlockScript = goRedis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

func generateUniqueValue() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generateUniqueValue: %w", err)
	}
	return hex.EncodeToString(b), nil
}

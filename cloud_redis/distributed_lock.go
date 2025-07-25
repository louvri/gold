package cloud_redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"
)

var (
	ErrDistributedLockNotAcquired = errors.New("distributed lock not acquired")
	ErrDistributedLockNotHeld     = errors.New("distributed lock not held by this client")
)

// WithRetryableDistributedLock executes a function while holding a distributed lock with retry mechanism
func (c *CloudRedis) WithRetryableDistributedLock(ctx context.Context, key string, fn func() error, timeout time.Duration, ttl ...time.Duration) error {
	lockTTL := 5 * time.Second
	if len(ttl) > 0 {
		lockTTL = ttl[0]
	}

	lockKey := fmt.Sprintf("lock:%s", key)
	lockValue := generateUniqueValue()

	err := c.acquireLockWithRetries(ctx, lockKey, lockValue, lockTTL, timeout)
	if err != nil {
		return err
	}

	defer func() {
		if unlockErr := c.releaseDistributedLock(context.Background(), lockKey, lockValue); unlockErr != nil {
			log.Printf("Failed to release lock %s: %v", key, unlockErr)
		}
	}()

	return fn()
}

// WithDistributedLock executes a function while holding a distributed lock
func (c *CloudRedis) WithDistributedLock(ctx context.Context, key string, fn func() error, ttl ...time.Duration) error {
	lockTTL := 5 * time.Second
	if len(ttl) > 0 {
		lockTTL = ttl[0]
	}

	lockKey := fmt.Sprintf("lock:%s", key)
	lockValue := generateUniqueValue()

	err := c.acquireDistributedLock(ctx, lockKey, lockValue, lockTTL)
	if err != nil {
		return err
	}

	defer func(tmpLockKey, tmpLockValue string) {
		if releaseErr := c.releaseDistributedLock(context.Background(), tmpLockKey, tmpLockValue); releaseErr != nil {
			log.Println("WithDistributedLock", "releaseDistributedLock", tmpLockKey, "error", releaseErr)
		}
	}(lockKey, lockValue)

	return fn()
}

// acquireLockWithRetries attempts to acquire the lock with retry mechanism
func (c *CloudRedis) acquireLockWithRetries(ctx context.Context, key, value string, ttl, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := c.acquireDistributedLock(ctx, key, value, ttl); !errors.Is(err, ErrDistributedLockNotAcquired) {
		return err
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ErrDistributedLockNotAcquired
		case <-ticker.C:
			if err := c.acquireDistributedLock(ctx, key, value, ttl); err != ErrDistributedLockNotAcquired {
				return err
			}
		}
	}
}

// acquireDistributedLock attempts to acquire the distributed lock immediately (fail-fast)
func (c *CloudRedis) acquireDistributedLock(ctx context.Context, key, value string, ttl time.Duration) error {
	result, err := c.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !result {
		return ErrDistributedLockNotAcquired
	}
	return nil
}

// releaseDistributedLock releases the distributed lock
func (c *CloudRedis) releaseDistributedLock(ctx context.Context, key, value string) error {
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`

	result, err := c.client.Eval(ctx, script, []string{key}, value).Result()
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	if val, ok := result.(int64); ok && val == 0 {
		return ErrDistributedLockNotHeld
	}

	return nil
}

func generateUniqueValue() string {
	bytes := make([]byte, 16)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

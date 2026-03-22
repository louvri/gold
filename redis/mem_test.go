package redis

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func setup(t *testing.T) (Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	r, err := New(mr.Host(), "", mr.Port())
	if err != nil {
		t.Fatal(err)
	}
	return r, mr
}

func setupReal(t *testing.T) Client {
	t.Helper()
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		t.Skip("skipping: set REDIS_HOST to run integration tests")
	}
	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	r, err := New(host, os.Getenv("REDIS_PASSWORD"), port)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// --- Set / Get ---

func TestSetAndGetData(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	if err := r.SetData(ctx, "greeting", "hello"); err != nil {
		t.Fatal(err)
	}
	val, err := r.GetData(ctx, "greeting")
	if err != nil {
		t.Fatal(err)
	}
	if val != "hello" {
		t.Fatalf("expected hello, got %s", val)
	}
}

func TestSetDataWithTTL(t *testing.T) {
	r, mr := setup(t)
	ctx := context.Background()

	if err := r.SetData(ctx, "temp", "value", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(6 * time.Second)
	_, err := r.GetData(ctx, "temp")
	if err == nil {
		t.Fatal("expected key to be expired")
	}
}

func TestGetDataNotFound(t *testing.T) {
	r, _ := setup(t)
	_, err := r.GetData(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

// --- SetNX ---

func TestSetNX(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	ok, err := r.SetNX(ctx, "unique", "first")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected first SetNX to succeed")
	}

	ok, err = r.SetNX(ctx, "unique", "second")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected second SetNX to fail")
	}

	val, _ := r.GetData(ctx, "unique")
	if val != "first" {
		t.Fatalf("expected first, got %s", val)
	}
}

func TestSetNXWithTTL(t *testing.T) {
	r, mr := setup(t)
	ctx := context.Background()

	ok, err := r.SetNX(ctx, "temp-unique", "val", 5*time.Second)
	if err != nil || !ok {
		t.Fatal("expected SetNX to succeed", err)
	}

	mr.FastForward(6 * time.Second)

	ok, err = r.SetNX(ctx, "temp-unique", "val2")
	if err != nil || !ok {
		t.Fatal("expected SetNX to succeed after expiry", err)
	}
}

// --- HSet / HGet ---

func TestHSetAndHGetData(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	if err := r.HSetData(ctx, "user:1", "name", "alice"); err != nil {
		t.Fatal(err)
	}
	val, err := r.HGetData(ctx, "user:1", "name")
	if err != nil {
		t.Fatal(err)
	}
	if val != "alice" {
		t.Fatalf("expected alice, got %s", val)
	}
}

func TestHSetDataWithTTL(t *testing.T) {
	r, mr := setup(t)
	ctx := context.Background()

	if err := r.HSetData(ctx, "session", "token", "abc123", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(6 * time.Second)
	_, err := r.HGetData(ctx, "session", "token")
	if err == nil {
		t.Fatal("expected key to be expired")
	}
}

func TestHSetDataValidation(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	if err := r.HSetData(ctx, "key", "only-one-arg"); err == nil {
		t.Fatal("expected error for insufficient args")
	}

	if err := r.HSetData(ctx, "key", "field", "val", "not-a-duration"); err == nil {
		t.Fatal("expected error for wrong ttl format")
	}
}

func TestHGetAllData(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	if err := r.HSetData(ctx, "user:2", "name", "bob"); err != nil {
		t.Fatal(err)
	}
	if err := r.HSetData(ctx, "user:2", "role", "admin"); err != nil {
		t.Fatal(err)
	}
	data, err := r.HGetAllData(ctx, "user:2")
	if err != nil {
		t.Fatal(err)
	}
	if data["name"] != "bob" || data["role"] != "admin" {
		t.Fatalf("unexpected data: %v", data)
	}
}

// --- Exists / Delete ---

func TestExists(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	exists, err := r.Exists(ctx, "nothing")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("expected key to not exist")
	}

	_ = r.SetData(ctx, "something", "val")
	exists, err = r.Exists(ctx, "something")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("expected key to exist")
	}
}

func TestDelete(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	_ = r.SetData(ctx, "to-delete", "val")
	if err := r.Delete(ctx, "to-delete"); err != nil {
		t.Fatal(err)
	}
	exists, _ := r.Exists(ctx, "to-delete")
	if exists {
		t.Fatal("expected key to be deleted")
	}
}

func TestHDelete(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	_ = r.HSetData(ctx, "hash", "f1", "v1")
	_ = r.HSetData(ctx, "hash", "f2", "v2")
	if err := r.HDelete(ctx, "hash", "f1"); err != nil {
		t.Fatal(err)
	}
	_, err := r.HGetData(ctx, "hash", "f1")
	if err == nil {
		t.Fatal("expected field to be deleted")
	}
	val, err := r.HGetData(ctx, "hash", "f2")
	if err != nil || val != "v2" {
		t.Fatal("expected f2 to still exist")
	}
}

// --- Incr / Decr ---

func TestIncrAndDecr(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	val, err := r.Incr(ctx, "counter")
	if err != nil || val != 1 {
		t.Fatalf("expected 1, got %d (err: %v)", val, err)
	}
	val, err = r.Incr(ctx, "counter")
	if err != nil || val != 2 {
		t.Fatalf("expected 2, got %d (err: %v)", val, err)
	}
	val, err = r.Decr(ctx, "counter")
	if err != nil || val != 1 {
		t.Fatalf("expected 1, got %d (err: %v)", val, err)
	}
}

// --- Expire / TTL ---

func TestExpireAndTTL(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	_ = r.SetData(ctx, "expiring", "val")
	ok, err := r.Expire(ctx, "expiring", 10*time.Second)
	if err != nil || !ok {
		t.Fatal("expected Expire to succeed", err)
	}
	d, err := r.TTL(ctx, "expiring")
	if err != nil {
		t.Fatal(err)
	}
	if d <= 0 || d > 10*time.Second {
		t.Fatalf("unexpected TTL: %v", d)
	}
}

// --- Scan ---

func TestScan(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	for i := range 5 {
		r.SetData(ctx, "scan:key:"+string(rune('a'+i)), "val")
	}
	r.SetData(ctx, "other:key", "val")

	keys, err := r.Scan(ctx, "scan:*", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 5 {
		t.Fatalf("expected 5 keys, got %d: %v", len(keys), keys)
	}
}

// --- Lock / Unlock (integration: requires real Redis for Lua scripts) ---

func TestLockAndUnlock(t *testing.T) {
	r := setupReal(t)
	ctx := context.Background()

	// Clean up test keys
	defer func() { _ = r.Delete(ctx, "test-lock-resource") }()

	ok, err := r.Lock(ctx, "test-lock-resource", "secret-1", 10*time.Second)
	if err != nil || !ok {
		t.Fatal("expected lock to succeed", err)
	}

	// Same secret can re-lock (idempotent)
	ok, err = r.Lock(ctx, "test-lock-resource", "secret-1", 10*time.Second)
	if err != nil || !ok {
		t.Fatal("expected re-lock with same secret to succeed", err)
	}

	// Different secret cannot lock
	ok, err = r.Lock(ctx, "test-lock-resource", "secret-2", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected lock with different secret to fail")
	}

	// Unlock with correct secret
	ok, err = r.Unlock(ctx, "test-lock-resource", "secret-1")
	if err != nil || !ok {
		t.Fatal("expected unlock to succeed", err)
	}

	// Now different secret can lock
	ok, err = r.Lock(ctx, "test-lock-resource", "secret-2", 10*time.Second)
	if err != nil || !ok {
		t.Fatal("expected lock after unlock to succeed", err)
	}

	// Clean up
	_, _ = r.Unlock(ctx, "test-lock-resource", "secret-2")
}

func TestUnlockWrongSecret(t *testing.T) {
	r := setupReal(t)
	ctx := context.Background()

	defer func() { _ = r.Delete(ctx, "test-lock-wrong") }()

	_, _ = r.Lock(ctx, "test-lock-wrong", "correct", 10*time.Second)
	ok, err := r.Unlock(ctx, "test-lock-wrong", "wrong")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected unlock with wrong secret to fail")
	}

	_, _ = r.Unlock(ctx, "test-lock-wrong", "correct")
}

// --- Distributed Lock ---

func TestWithDistributedLock(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	executed := false
	result, err := r.WithDistributedLock(ctx, "job-1", func() (any, error) {
		executed = true
		return "done", nil
	}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !executed {
		t.Fatal("expected function to be executed")
	}
	if result != "done" {
		t.Fatalf("expected done, got %v", result)
	}

	// Lock should be released — can acquire again
	_, err = r.WithDistributedLock(ctx, "job-1", func() (any, error) {
		return nil, nil
	}, 5*time.Second)
	if err != nil {
		t.Fatal("expected second lock to succeed after release", err)
	}
}

func TestWithDistributedLockContention(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	// Hold a lock manually
	_, _ = r.SetNX(ctx, "lock:contended", "someone-else", 30*time.Second)

	// Attempting to acquire should fail
	_, err := r.WithDistributedLock(ctx, "contended", func() (any, error) {
		t.Fatal("should not execute")
		return nil, nil
	})
	if !errors.Is(err, ErrDistributedLockNotAcquired) {
		t.Fatalf("expected ErrDistributedLockNotAcquired, got %v", err)
	}
}

func TestWithRetryableDistributedLock(t *testing.T) {
	r, mr := setup(t)
	ctx := context.Background()

	// Hold a lock that will expire
	_, _ = r.SetNX(ctx, "lock:retry-job", "holder", 1*time.Second)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mr.FastForward(2 * time.Second)
	}()

	result, err := r.WithRetryableDistributedLock(ctx, "retry-job", func() (any, error) {
		return "acquired", nil
	}, 5*time.Second, 100*time.Millisecond, 10*time.Second)

	wg.Wait()

	if err != nil {
		t.Fatal(err)
	}
	if result != "acquired" {
		t.Fatalf("expected acquired, got %v", result)
	}
}

func TestWithDistributedLockFnError(t *testing.T) {
	r, _ := setup(t)
	ctx := context.Background()

	expectedErr := errors.New("fn failed")
	_, err := r.WithDistributedLock(ctx, "error-job", func() (any, error) {
		return nil, expectedErr
	}, 5*time.Second)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected fn error, got %v", err)
	}

	// Lock should still be released after fn error
	_, err = r.WithDistributedLock(ctx, "error-job", func() (any, error) {
		return "ok", nil
	}, 5*time.Second)
	if err != nil {
		t.Fatal("expected lock to be released after fn error", err)
	}
}

// --- Helpers ---

func TestToInt(t *testing.T) {
	tests := []struct {
		input    any
		expected int
		wantErr  bool
	}{
		{int(42), 42, false},
		{int64(99), 99, false},
		{float64(7.0), 7, false},
		{"123", 123, false},
		{"not-a-number", 0, true},
		{nil, 0, true},
		{true, 0, true},
	}
	for _, tc := range tests {
		result, err := toInt(tc.input)
		if tc.wantErr && err == nil {
			t.Errorf("toInt(%v): expected error", tc.input)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("toInt(%v): unexpected error: %v", tc.input, err)
		}
		if result != tc.expected {
			t.Errorf("toInt(%v): expected %d, got %d", tc.input, tc.expected, result)
		}
	}
}

func TestClient(t *testing.T) {
	r, _ := setup(t)
	if r.RedisClient() == nil {
		t.Fatal("expected non-nil client")
	}
}

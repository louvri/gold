# redis

Redis client wrapper with support for caching, session locking, and distributed locks.

## Installation

```
go get github.com/louvri/gold/redis
```

Requires Go 1.25 or later.

## Usage

### Create Client

```go
import "github.com/louvri/gold/redis"

client, err := redis.New("localhost", "", "6379")
```

### Caching

```go
ctx := context.Background()

// Set with optional TTL
err := client.SetData(ctx, "key", "value", 30*time.Minute)

// Get
val, err := client.GetData(ctx, "key")

// Hash set / get
err = client.HSetData(ctx, "user:1", "name", "alice")
name, err := client.HGetData(ctx, "user:1", "name")
all, err := client.HGetAllData(ctx, "user:1")

// Set if not exists
ok, err := client.SetNX(ctx, "unique-key", "value", 10*time.Second)

// Other operations
exists, err := client.Exists(ctx, "key")
err = client.Delete(ctx, "key")
err = client.HDelete(ctx, "user:1", "name")
val, err := client.Incr(ctx, "counter")
val, err = client.Decr(ctx, "counter")
ok, err = client.Expire(ctx, "key", 1*time.Hour)
ttl, err := client.TTL(ctx, "key")
keys, err := client.Scan(ctx, "prefix:*", 100)
```

### Session Lock

Reentrant lock using a caller-provided secret. The same secret can re-lock (idempotent), but a different secret will be rejected while the lock is held. The secret must be non-empty and unique to its holder: `Lock` returns `ErrEmptyLockSecret` for an empty one, since it would make every caller the owner. (`Unlock` still accepts `""`, so a lock taken that way by an older release can be released; it can never release another holder's lock.) The TTL defaults to 24h and is rounded up to whole seconds.

```go
ctx := context.Background()

ok, err := client.Lock(ctx, "resource", "my-secret", 10*time.Second)
if ok {
    // do work
}

ok, err = client.Unlock(ctx, "resource", "my-secret")
```

### Distributed Lock

Automatically acquires and releases a lock around a function call using a unique token. The TTL defaults to 5s and must be positive: a zero TTL used to store the lock without expiry, so a crashed holder kept it forever, and now returns `ErrInvalidLockTTL`. With retry, the retry period must be positive too.

```go
ctx := context.Background()

result, err := client.WithDistributedLock(ctx, "job-key", func() (any, error) {
    // critical section
    return "done", nil
}, 5*time.Second)
```

With retry (waits for the lock to become available):

```go
result, err := client.WithRetryableDistributedLock(ctx, "job-key", func() (any, error) {
    return "done", nil
}, 10*time.Second, 500*time.Millisecond, 5*time.Second)
// timeout: 10s, retry interval: 500ms, lock TTL: 5s
```

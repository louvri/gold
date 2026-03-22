# gold

`gold` is a Go library that provides simple wrappers for common Google Cloud services.

Currently supports:
- [Cloud PubSub](pubsub/) — publish and subscribe to topics
- [Cloud Memorystore (Redis)](redis/) — caching, locking, and distributed locks
- [Cloud Storage](storage/) — upload, download, and manage objects

## Installation

Each service is a separate Go module. Install only what you need:

```
go get github.com/louvri/gold/pubsub
go get github.com/louvri/gold/redis
go get github.com/louvri/gold/storage
```

See each module's README for usage details.

# pubsub

Google Cloud PubSub client wrapper for publishing and subscribing to topics.

## Installation

```
go get github.com/louvri/gold/pubsub
```

## Usage

### Publisher

```go
import "github.com/louvri/gold/pubsub"

publisher, err := pubsub.NewPublisher("project-id", "topic-id", "")
// Pass credentials JSON as the third argument, or leave empty for default credentials.
defer publisher.Close()

msgID, err := publisher.PublishMessage(ctx, pubsub.Data{
    ID:          "order-123",
    Publisher:   "order-service",
    Action:      "created",
    RequestTime: time.Now().UTC().Format("2006-01-02 15:04:05"),
    Data:        map[string]string{"item": "book"},
})

// With attributes
msgID, err = publisher.PublishMessageWithAttributes(ctx, data, map[string]string{
    "type":     "order",
    "priority": "high",
})
```

### Subscriber

```go
subscriber, err := pubsub.NewSubscriber("project-id", "subscription-id", "")
defer subscriber.Close()

err = subscriber.Receive(ctx, func(ctx context.Context, msg *gpubsub.Message) {
    log.Printf("received: %s", msg.Data)
    msg.Ack()
})
```

With concurrency limits:

```go
subscriber, err := pubsub.NewSubscriber("project-id", "subscription-id", "",
    pubsub.SubscriberOption{
        MaxOutstandingMessages: 100,
        NumGoroutines:          4,
    },
)
```

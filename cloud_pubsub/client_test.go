package cloud_pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"log"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublishMessage(t *testing.T) {
	publisher := NewCloudPublisher("my-project", "my-topic", "")
	if publisher.TopicID == "" {
		t.Fatal(errors.New("failed to initialize publisher"))
	}
	messageId, err := publisher.PublishMessage(context.Background(), Data{
		Id:          "IDE123456789",
		Publisher:   "user1",
		Action:      "scan-pickup",
		RequestTime: time.Now().UTC().Format("2006-01-02 15:04:05"),
		Data:        nil,
	})
	if err != nil || messageId == "" {
		t.Fatal(err)
	}
	nextMessageId, err := publisher.PublishMessage(context.Background(), Data{
		Id:          "IDP123456789",
		Publisher:   "user1",
		Action:      "scan-sending",
		RequestTime: time.Now().UTC().Format("2006-01-02 15:04:05"),
		Data:        nil,
	})
	if err != nil || nextMessageId == "" {
		t.Fatal(err)
	}
}

func TestSubscription(t *testing.T) {
	subscriber := NewCloudSubscriber("my-project", "my-subscription", "")
	if subscriber.SubscriptionID == "" {
		t.Fatal(errors.New("failed to initialize subscriber"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var received int32
	err := subscriber.Subscription.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		log.Printf("Got message: %q\n", string(msg.Data))
		atomic.AddInt32(&received, 1)
		msg.Ack()
	})
	if err != nil {
		t.Fatal(err)
	}
	if received == 0 {
		t.Fatal(errors.New("failed to receive subscription"))
	}
}

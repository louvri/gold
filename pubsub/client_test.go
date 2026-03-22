package pubsub

import (
	"context"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

func skipWithoutGCP(t *testing.T) {
	t.Helper()
	if os.Getenv("GCP_PROJECT_ID") == "" {
		t.Skip("skipping: set GCP_PROJECT_ID to run integration tests")
	}
}

func TestIntegrationPublishMessage(t *testing.T) {
	skipWithoutGCP(t)
	projectID := os.Getenv("GCP_PROJECT_ID")

	publisher, err := NewPublisher(projectID, "my-topic", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()

	msgID, err := publisher.PublishMessage(context.Background(), Data{
		ID:          "IDE123456789",
		Publisher:   "user1",
		Action:      "scan-pickup",
		RequestTime: time.Now().UTC().Format("2006-01-02 15:04:05"),
	})
	if err != nil || msgID == "" {
		t.Fatal("expected message ID", err)
	}
}

func TestIntegrationSubscription(t *testing.T) {
	skipWithoutGCP(t)
	projectID := os.Getenv("GCP_PROJECT_ID")

	subscriber, err := NewSubscriber(projectID, "my-subscription", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscriber.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var received int32
	err = subscriber.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		log.Printf("Got message: %q\n", string(msg.Data))
		atomic.AddInt32(&received, 1)
		msg.Ack()
	})
	if err != nil {
		t.Fatal(err)
	}
	if received == 0 {
		t.Fatal("failed to receive subscription")
	}
}

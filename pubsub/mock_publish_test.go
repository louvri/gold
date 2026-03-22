package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"
)

type MockPublisher struct {
	mu         sync.Mutex
	messages   []Data
	attributes []map[string]string
	err        error
}

func (p *MockPublisher) PublishMessage(ctx context.Context, data Data) (string, error) {
	return p.PublishMessageWithAttributes(ctx, data, nil)
}

func (p *MockPublisher) PublishMessageWithAttributes(_ context.Context, data Data, attrs map[string]string) (string, error) {
	if p.err != nil {
		return "", p.err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, data)
	p.attributes = append(p.attributes, attrs)
	return "msg-" + data.ID, nil
}

func (p *MockPublisher) Close() error {
	return nil
}

func TestMockPublishMessage(t *testing.T) {
	pub := &MockPublisher{}
	var publisher Publisher = pub

	msgID, err := publisher.PublishMessage(context.Background(), Data{
		ID:          "order-001",
		Publisher:   "order-service",
		Action:      "created",
		RequestTime: time.Now().UTC().Format("2006-01-02 15:04:05"),
		Data:        map[string]string{"item": "book"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if msgID != "msg-order-001" {
		t.Fatalf("expected msg-order-001, got %s", msgID)
	}
	if len(pub.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(pub.messages))
	}
	if pub.messages[0].Action != "created" {
		t.Fatalf("expected action created, got %s", pub.messages[0].Action)
	}
}

func TestMockPublishMessageWithAttributes(t *testing.T) {
	pub := &MockPublisher{}
	var publisher Publisher = pub

	attrs := map[string]string{"type": "order", "priority": "high"}
	msgID, err := publisher.PublishMessageWithAttributes(context.Background(), Data{
		ID:     "order-002",
		Action: "shipped",
	}, attrs)
	if err != nil {
		t.Fatal(err)
	}
	if msgID != "msg-order-002" {
		t.Fatalf("expected msg-order-002, got %s", msgID)
	}
	if pub.attributes[0]["priority"] != "high" {
		t.Fatal("expected priority attribute to be high")
	}
}

func TestMockPublishMultipleMessages(t *testing.T) {
	pub := &MockPublisher{}
	var publisher Publisher = pub

	actions := []string{"created", "updated", "deleted"}
	for i, action := range actions {
		_, err := publisher.PublishMessage(context.Background(), Data{
			ID:     "item-" + string(rune('1'+i)),
			Action: action,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(pub.messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(pub.messages))
	}
}

func TestMockPublishError(t *testing.T) {
	pub := &MockPublisher{err: context.DeadlineExceeded}
	var publisher Publisher = pub

	_, err := publisher.PublishMessage(context.Background(), Data{ID: "fail"})
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestMockPublisherClose(t *testing.T) {
	pub := &MockPublisher{}
	if err := pub.Close(); err != nil {
		t.Fatal(err)
	}
}

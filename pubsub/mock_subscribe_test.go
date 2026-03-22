package pubsub

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/pubsub"
)

type MockSubscriber struct {
	callback func(ctx context.Context, msg *pubsub.Message)
	err      error
	closed   bool
}

func (s *MockSubscriber) Receive(_ context.Context, process func(ctx context.Context, msg *pubsub.Message)) error {
	if s.err != nil {
		return s.err
	}
	s.callback = process
	return nil
}

func (s *MockSubscriber) Close() error {
	s.closed = true
	return nil
}

func (s *MockSubscriber) SimulateMessage(data []byte) {
	s.callback(context.Background(), &pubsub.Message{Data: data})
}

func TestMockSubscribeAndReceive(t *testing.T) {
	sub := &MockSubscriber{}
	var subscriber Subscriber = sub

	var received int32
	err := subscriber.Receive(context.Background(), func(_ context.Context, msg *pubsub.Message) {
		atomic.AddInt32(&received, 1)
	})
	if err != nil {
		t.Fatal(err)
	}

	sub.SimulateMessage([]byte("hello"))
	sub.SimulateMessage([]byte("world"))

	if received != 2 {
		t.Fatalf("expected 2 messages, got %d", received)
	}
}

func TestMockSubscribeMessageContent(t *testing.T) {
	sub := &MockSubscriber{}
	var subscriber Subscriber = sub

	var lastData string
	err := subscriber.Receive(context.Background(), func(_ context.Context, msg *pubsub.Message) {
		lastData = string(msg.Data)
	})
	if err != nil {
		t.Fatal(err)
	}

	sub.SimulateMessage([]byte("test-payload"))
	if lastData != "test-payload" {
		t.Fatalf("expected test-payload, got %s", lastData)
	}
}

func TestMockSubscribeError(t *testing.T) {
	expectedErr := errors.New("connection lost")
	sub := &MockSubscriber{err: expectedErr}
	var subscriber Subscriber = sub

	err := subscriber.Receive(context.Background(), func(_ context.Context, _ *pubsub.Message) {})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected connection lost error, got %v", err)
	}
}

func TestMockSubscriberClose(t *testing.T) {
	sub := &MockSubscriber{}
	var subscriber Subscriber = sub

	if err := subscriber.Close(); err != nil {
		t.Fatal(err)
	}
	if !sub.closed {
		t.Fatal("expected subscriber to be closed")
	}
}

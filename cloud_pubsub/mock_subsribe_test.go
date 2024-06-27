package cloud_pubsub

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"testing"

	message "cloud.google.com/go/pubsub"
)

var received int32

// mocking instance

type MockCloudSubscriber struct {
	tmp          func(ctx context.Context, msg *message.Message)
	TopicID      string
	SubscriberID string
}

func (s *MockCloudSubscriber) Receive(ctx context.Context, process func(ctx context.Context, msg *message.Message)) error {
	s.tmp = process
	return nil
}

func (s *MockCloudSubscriber) Call(msg *message.Message) {
	s.tmp(context.Background(), msg)
}

func NewMock() *MockCloudSubscriber {
	m := new(MockCloudSubscriber)
	m.TopicID = "my-topic"
	m.SubscriberID = "my-subscriber"
	return m
}

// Dummy Subscribers

type testSubscriber struct {
	subs Subscriber
}

func (t *testSubscriber) ListenAndServe() error {
	return t.subs.Receive(context.Background(), func(_ context.Context, msg *message.Message) {
		log.Printf("Got message: %q\n", string(msg.Data))
		atomic.AddInt32(&received, 1)
		msg.Ack()
	})
}

func NewTestSubs(subs Subscriber) *testSubscriber {
	return &testSubscriber{
		subs: subs,
	}
}

func TestMockSubscription(t *testing.T) {
	subs := NewMock()
	r := NewTestSubs(subs)
	err := r.ListenAndServe()
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	subs.Call(&message.Message{
		Data: []byte("my message"),
	})
	if received == 0 {
		t.Fatal(errors.New("failed to receive subscription"))
	}
}

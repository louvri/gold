package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/api/option"
)

type Subscriber interface {
	Receive(context.Context, func(ctx context.Context, msg *pubsub.Message)) error
	Close() error
}

type SubscriberOption struct {
	MaxOutstandingMessages int
	NumGoroutines          int
}

func NewSubscriber(projectID, subscriptionID, credentialsJSON string, opts ...SubscriberOption) (Subscriber, error) {
	ctx := context.Background()
	var clientOpts []option.ClientOption
	if credentialsJSON != "" {
		clientOpts = append(clientOpts, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(credentialsJSON)))
	}
	client, err := pubsub.NewClient(ctx, projectID, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("subscriber.NewClient: %w", err)
	}
	subscriber := client.Subscriber(subscriptionID)
	if len(opts) > 0 {
		subscriber.ReceiveSettings.MaxOutstandingMessages = opts[0].MaxOutstandingMessages
		subscriber.ReceiveSettings.NumGoroutines = opts[0].NumGoroutines
	}
	return &cloudSubscriber{
		projectID:      projectID,
		subscriptionID: subscriptionID,
		client:         client,
		subscriber:     subscriber,
	}, nil
}

// NewSubscriberWithLimit creates a subscriber with concurrency limits.
// Deprecated: Use NewSubscriber with SubscriberOption instead.
func NewSubscriberWithLimit(projectID, subscriptionID, credentialsJSON string, maxOutstandingMessages, numGoroutines int) (Subscriber, error) {
	return NewSubscriber(projectID, subscriptionID, credentialsJSON, SubscriberOption{
		MaxOutstandingMessages: maxOutstandingMessages,
		NumGoroutines:          numGoroutines,
	})
}

type cloudSubscriber struct {
	projectID      string
	subscriptionID string
	client         *pubsub.Client
	subscriber     *pubsub.Subscriber
}

func (s *cloudSubscriber) Receive(ctx context.Context, callback func(ctx context.Context, msg *pubsub.Message)) error {
	return s.subscriber.Receive(ctx, callback)
}

func (s *cloudSubscriber) Close() error {
	return s.client.Close()
}

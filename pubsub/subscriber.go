package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
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
		clientOpts = append(clientOpts, option.WithCredentialsJSON([]byte(credentialsJSON)))
	}
	client, err := pubsub.NewClient(ctx, projectID, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("subscriber.NewClient: %w", err)
	}
	subscription := client.Subscription(subscriptionID)
	if len(opts) > 0 {
		subscription.ReceiveSettings.MaxOutstandingMessages = opts[0].MaxOutstandingMessages
		subscription.ReceiveSettings.NumGoroutines = opts[0].NumGoroutines
	}
	return &cloudSubscriber{
		projectID:      projectID,
		subscriptionID: subscriptionID,
		client:         client,
		subscription:   subscription,
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
	subscription   *pubsub.Subscription
}

func (s *cloudSubscriber) Receive(ctx context.Context, callback func(ctx context.Context, msg *pubsub.Message)) error {
	return s.subscription.Receive(ctx, callback)
}

func (s *cloudSubscriber) Close() error {
	return s.client.Close()
}

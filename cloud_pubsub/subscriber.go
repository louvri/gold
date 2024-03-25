package cloud_pubsub

import (
	"context"
	"log"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type Subscriber interface {
	Receive(context.Context, func(ctx context.Context, msg *pubsub.Message)) error
}

func NewCloudSubscriber(projectID, subscriptionID, credentialsJSON string) (Subscriber, error) {
	ctx := context.Background()
	var client *pubsub.Client
	var err error
	if credentialsJSON != "" {
		client, err = pubsub.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(credentialsJSON)))
	} else {
		client, err = pubsub.NewClient(ctx, projectID)
	}
	if err != nil {
		log.Printf("subscriber.NewClient err: %v\n", err)
		return nil, err
	}
	cleanUpCloudClient(client)
	subscription := client.Subscription(subscriptionID)
	return &CloudSubscriber{
		ProjectID:      projectID,
		SubscriptionID: subscriptionID,
		Subscription:   subscription,
	}, nil
}

type CloudSubscriber struct {
	ProjectID      string
	SubscriptionID string
	Subscription   *pubsub.Subscription
}

func (s *CloudSubscriber) Receive(ctx context.Context, callback func(ctx context.Context, msg *pubsub.Message)) error {
	return s.Subscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		callback(ctx, m)
	})
}

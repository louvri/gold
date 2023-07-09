package cloud_pubsub

import "cloud.google.com/go/pubsub"

type (
	CloudSubscriber struct {
		ProjectID      string
		SubscriptionID string
		Subscription   *pubsub.Subscription
	}
	CloudPublisher struct {
		ProjectID string
		TopicID   string
		Client    *pubsub.Client
	}
	Data struct {
		Id          string
		Publisher   string
		Action      string
		RequestTime string
		Data        interface{}
	}
)

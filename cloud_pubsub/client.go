package cloud_pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"google.golang.org/api/option"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func NewCloudSubscriber(projectID, subscriptionID, credentialsJSON string) CloudSubscriber {
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
		return CloudSubscriber{}
	}
	cleanUpCloudClient(client)
	subscription := client.Subscription(subscriptionID)
	return CloudSubscriber{
		ProjectID:      projectID,
		SubscriptionID: subscriptionID,
		Subscription:   subscription,
	}
}

func NewCloudPublisher(projectID, topicID, credentialsJSON string) CloudPublisher {
	ctx := context.Background()
	var client *pubsub.Client
	var err error
	if credentialsJSON != "" {
		client, err = pubsub.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(credentialsJSON)))
	} else {
		client, err = pubsub.NewClient(ctx, projectID)
	}
	if err != nil {
		log.Printf("publisher.NewClient err: %v\n", err)
		return CloudPublisher{}
	}
	cleanUpCloudClient(client)
	return CloudPublisher{
		ProjectID: projectID,
		TopicID:   topicID,
		Client:    client,
	}
}

func (cp *CloudPublisher) PublishMessage(ctx context.Context, pubSubData Data) (messageID string, err error) {
	topic := cp.Client.Topic(cp.TopicID)
	byteData, _ := json.Marshal(pubSubData)
	result := topic.Publish(ctx, &pubsub.Message{
		Data: byteData,
	})

	messageID, err = result.Get(ctx)
	if err != nil {
		log.Printf("publisher.Get: %v\n", err)
		return messageID, err
	}
	return messageID, nil
}

func cleanUpCloudClient(client *pubsub.Client) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		_ = client.Close()
		os.Exit(0)
	}()
}

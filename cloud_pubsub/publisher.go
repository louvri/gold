package cloud_pubsub

import (
	"context"
	"encoding/json"
	"log"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type Data struct {
	Id          string
	Publisher   string
	Action      string
	RequestTime string
	Data        interface{}
}

type Publisher interface {
	PublishMessage(ctx context.Context, data Data) (messageID string, err error)
}

func NewCloudPublisher(projectID, topicID, credentialsJSON string) (Publisher, error) {
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
		return nil, err
	}
	cleanUpCloudClient(client)
	return &CloudPublisher{
		ProjectID: projectID,
		TopicID:   topicID,
		Client:    client,
	}, nil
}

type CloudPublisher struct {
	ProjectID string
	TopicID   string
	Client    *pubsub.Client
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

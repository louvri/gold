package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/api/option"
)

type Data struct {
	ID          string
	Publisher   string
	Action      string
	RequestTime string
	Data        any
}

type Publisher interface {
	PublishMessage(ctx context.Context, data Data) (messageID string, err error)
	PublishMessageWithAttributes(ctx context.Context, data Data, attributes map[string]string) (messageID string, err error)
	Close() error
}

func NewPublisher(projectID, topicID, credentialsJSON string) (Publisher, error) {
	ctx := context.Background()
	var opts []option.ClientOption
	if credentialsJSON != "" {
		opts = append(opts, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(credentialsJSON)))
	}
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("publisher.NewClient: %w", err)
	}
	return &cloudPublisher{
		projectID: projectID,
		topicID:   topicID,
		client:    client,
		publisher: client.Publisher(topicID),
	}, nil
}

type cloudPublisher struct {
	projectID string
	topicID   string
	client    *pubsub.Client
	publisher *pubsub.Publisher
}

func (cp *cloudPublisher) PublishMessage(ctx context.Context, pubSubData Data) (string, error) {
	return cp.PublishMessageWithAttributes(ctx, pubSubData, nil)
}

func (cp *cloudPublisher) PublishMessageWithAttributes(ctx context.Context, pubSubData Data, attributes map[string]string) (string, error) {
	byteData, err := json.Marshal(pubSubData)
	if err != nil {
		return "", fmt.Errorf("publisher.Marshal: %w", err)
	}
	result := cp.publisher.Publish(ctx, &pubsub.Message{
		Data:       byteData,
		Attributes: attributes,
	})
	messageID, err := result.Get(ctx)
	if err != nil {
		return "", fmt.Errorf("publisher.Get: %w", err)
	}
	return messageID, nil
}

func (cp *cloudPublisher) Close() error {
	cp.publisher.Stop()
	return cp.client.Close()
}

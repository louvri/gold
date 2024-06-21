package cloud_pubsub

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type Mock struct {
	ProjectID string
	TopicID   string
}

func (p *Mock) PublishMessage(ctx context.Context, data Data) (messageID string, err error) {
	fmt.Println("pubsub message:")
	fmt.Println(data)
	return "msg1234", nil
}

func NewMockPubs() *Mock {
	return &Mock{
		ProjectID: "my-project",
		TopicID:   "my-topic",
	}
}
func TestPublishMessage(t *testing.T) {
	// tmp, _ := NewCloudPublisher("my-project", "my-topic", "")
	publisher := NewMockPubs()
	if publisher.TopicID == "" {
		t.Fatal(errors.New("failed to initialize publisher"))
	}
	messageId, err := publisher.PublishMessage(context.Background(), Data{
		Id:          "IDE123456789",
		Publisher:   "user1",
		Action:      "scan-pickup",
		RequestTime: time.Now().UTC().Format("2006-01-02 15:04:05"),
		Data:        nil,
	})
	if err != nil || messageId == "" {
		t.Fatal(err)
	}
	nextMessageId, err := publisher.PublishMessage(context.Background(), Data{
		Id:          "IDP123456789",
		Publisher:   "user1",
		Action:      "scan-sending",
		RequestTime: time.Now().UTC().Format("2006-01-02 15:04:05"),
		Data:        nil,
	})
	if err != nil || nextMessageId == "" {
		t.Fatal(err)
	}
}

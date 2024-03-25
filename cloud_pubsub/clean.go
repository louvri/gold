package cloud_pubsub

import (
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
)

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

package cloud_storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type Storage interface {
	UploadByPath(ctx context.Context, path string) (*string, error)
}

func New(environment, credential, contentType, bucketName, customEndpoint string, expiryDuration time.Duration) Storage {
	var opt []option.ClientOption
	if customEndpoint != "" {
		opt = append(opt, option.WithEndpoint(customEndpoint))
	}
	_, err := os.Stat(credential)
	if err != nil {
		opt = append(opt, option.WithCredentialsJSON([]byte(credential)))
	} else {
		opt = append(opt, option.WithCredentialsFile(credential))
	}

	return &CloudStorage{
		ExpiryDuration: expiryDuration,
		ContentType:    contentType,
		Option:         opt,
		Bucket:         bucketName,
	}
}

type CloudStorage struct {
	ExpiryDuration time.Duration
	ContentType    string
	Option         []option.ClientOption
	Bucket         string
}

func (c *CloudStorage) UploadByPath(ctx context.Context, path string) (*string, error) {
	var err error
	var client *storage.Client
	if c.Option != nil {
		client, err = storage.NewClient(context.Background(), c.Option...)
	} else {
		client, err = storage.NewClient(context.Background())
	}
	if err != nil {
		return nil, err
	}
	defer func(client *storage.Client) {
		_ = client.Close()
	}(client)
	bufferSize := 1024
	_, filename := filepath.Split(path)
	writer := client.Bucket(c.Bucket).Object(filename).NewWriter(ctx)
	writer.ContentType = c.ContentType
	writer.ChunkSize = bufferSize
	defer func(writer *storage.Writer) {
		_ = writer.Close()
	}(writer)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	if _, err = io.Copy(writer, file); err != nil && err != io.EOF {
		return nil, err
	}

	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(c.ExpiryDuration),
	}
	uri, err := client.Bucket(c.Bucket).SignedURL(filename, opts)
	if err != nil {
		return nil, err
	}
	return &uri, nil
}

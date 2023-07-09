package cloud_storage

import (
	"cloud.google.com/go/storage"
	"context"
	"io"
	"os"
	"path/filepath"
	"time"
)

func (cs *CloudStorage) UploadByPath(ctx context.Context, path string) (*string, error) {
	var err error
	var client *storage.Client
	if cs.Option != nil {
		client, err = storage.NewClient(context.Background(), cs.Option...)
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
	writer := client.Bucket(cs.Bucket).Object(filename).NewWriter(ctx)
	writer.ContentType = cs.ContentType
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
		Expires: time.Now().Add(cs.ExpiryDuration),
	}
	uri, err := client.Bucket(cs.Bucket).SignedURL(filename, opts)
	if err != nil {
		return nil, err
	}
	return &uri, nil
}

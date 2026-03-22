package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Client interface {
	UploadByPath(ctx context.Context, path string) (*string, error)
	UploadFromReader(ctx context.Context, objectName string, reader io.Reader) (*string, error)
	DownloadToPath(ctx context.Context, objectName, destPath string) error
	Download(ctx context.Context, objectName string, writer io.Writer) error
	Delete(ctx context.Context, objectName string) error
	List(ctx context.Context, prefix string) ([]string, error)
	Exists(ctx context.Context, objectName string) (bool, error)
}

func New(credential, contentType, bucketName, customEndpoint string, expiryDuration time.Duration) (Client, error) {
	var opts []option.ClientOption
	if customEndpoint != "" {
		opts = append(opts, option.WithEndpoint(customEndpoint))
	}
	if _, err := os.Stat(credential); err != nil {
		opts = append(opts, option.WithCredentialsJSON([]byte(credential)))
	} else {
		opts = append(opts, option.WithCredentialsFile(credential))
	}

	client, err := gcs.NewClient(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	return &cloudClient{
		client:         client,
		expiryDuration: expiryDuration,
		contentType:    contentType,
		bucket:         bucketName,
	}, nil
}

type cloudClient struct {
	client         *gcs.Client
	expiryDuration time.Duration
	contentType    string
	bucket         string
}

func (c *cloudClient) UploadByPath(ctx context.Context, path string) (*string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	_, filename := filepath.Split(path)
	return c.UploadFromReader(ctx, filename, file)
}

func (c *cloudClient) UploadFromReader(ctx context.Context, objectName string, reader io.Reader) (*string, error) {
	writer := c.client.Bucket(c.bucket).Object(objectName).NewWriter(ctx)
	writer.ContentType = c.contentType
	if _, err := io.Copy(writer, reader); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("upload copy %s: %w", objectName, err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("upload close %s: %w", objectName, err)
	}

	uri, err := c.client.Bucket(c.bucket).SignedURL(objectName, &gcs.SignedURLOptions{
		Scheme:  gcs.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(c.expiryDuration),
	})
	if err != nil {
		return nil, err
	}
	return &uri, nil
}

func (c *cloudClient) DownloadToPath(ctx context.Context, objectName, destPath string) error {
	file, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	return c.Download(ctx, objectName, file)
}

func (c *cloudClient) Download(ctx context.Context, objectName string, writer io.Writer) error {
	reader, err := c.client.Bucket(c.bucket).Object(objectName).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("download open %s: %w", objectName, err)
	}
	defer func() { _ = reader.Close() }()

	if _, err := io.Copy(writer, reader); err != nil {
		return fmt.Errorf("download copy %s: %w", objectName, err)
	}
	return nil
}

func (c *cloudClient) Delete(ctx context.Context, objectName string) error {
	if err := c.client.Bucket(c.bucket).Object(objectName).Delete(ctx); err != nil {
		return fmt.Errorf("delete %s: %w", objectName, err)
	}
	return nil
}

func (c *cloudClient) List(ctx context.Context, prefix string) ([]string, error) {
	it := c.client.Bucket(c.bucket).Objects(ctx, &gcs.Query{Prefix: prefix})
	var names []string
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		names = append(names, attrs.Name)
	}
	return names, nil
}

func (c *cloudClient) Exists(ctx context.Context, objectName string) (bool, error) {
	_, err := c.client.Bucket(c.bucket).Object(objectName).Attrs(ctx)
	if errors.Is(err, gcs.ErrObjectNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("exists %s: %w", objectName, err)
	}
	return true, nil
}

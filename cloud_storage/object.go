package cloud_storage

import (
	"google.golang.org/api/option"
	"os"
	"time"
)

type CloudStorage struct {
	ExpiryDuration time.Duration
	ContentType    string
	Option         []option.ClientOption
	Bucket         string
}

func New(environment, credential, contentType, bucketName, customEndpoint string, expiryDuration time.Duration) CloudStorage {
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

	return CloudStorage{
		ExpiryDuration: expiryDuration,
		ContentType:    contentType,
		Option:         opt,
		Bucket:         bucketName,
	}
}

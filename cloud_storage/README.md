# Usage  

## Installation
```
go get cloud.google.com/go/storage
go get google.golang.org/api/option
go get github.com/louvri/gold
```

## Initiate Object
### Import Object
```
import (
    cloudStorage github.com/louvri/gold/cloud_storage
)
```
### Create Object
```
environment := "stg"
credential := "xxx"
contentType := "text/csv"
bucketName := "gcs-asia-southeast2-xxx"
expiryDuration := 24 * time.Hour
storage := cloudStorage.New(environment, credential, contentType, bucketName, "", expiryDuration)
```  

## Signed URL Actions
### Upload File
```
ctx := context.Background()
filePath := "file.txt"
fileURL, err := cloudStorage.UploadByPath(ctx, filePath)
```  

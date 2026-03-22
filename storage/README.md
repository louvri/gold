# storage

Google Cloud Storage client wrapper for uploading, downloading, and managing objects.

## Installation

```
go get github.com/louvri/gold/storage
```

## Usage

### Create Client

```go
import "github.com/louvri/gold/storage"

client, err := storage.New(
    "credentials.json",       // path to file or raw JSON string
    "text/csv",               // content type for uploads
    "my-bucket",              // bucket name
    "",                       // custom endpoint (optional, leave empty for default)
    24*time.Hour,             // signed URL expiry duration
)
```

### Upload

```go
ctx := context.Background()

// Upload from file path
url, err := client.UploadByPath(ctx, "/path/to/file.csv")

// Upload from reader
url, err := client.UploadFromReader(ctx, "object-name.csv", reader)
```

Both return a signed URL for the uploaded object.

### Download

```go
// Download to file
err := client.DownloadToPath(ctx, "object-name.csv", "/path/to/dest.csv")

// Download to writer
var buf bytes.Buffer
err := client.Download(ctx, "object-name.csv", &buf)
```

### Manage Objects

```go
// List objects by prefix
names, err := client.List(ctx, "reports/")

// Check existence
exists, err := client.Exists(ctx, "object-name.csv")

// Delete
err := client.Delete(ctx, "object-name.csv")
```

package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

type mockStorage struct {
	objects map[string][]byte
	err     error
}

func newMockStorage() *mockStorage {
	return &mockStorage{objects: make(map[string][]byte)}
}

func (m *mockStorage) UploadByPath(_ context.Context, path string) (*string, error) {
	if m.err != nil {
		return nil, m.err
	}
	uri := "https://storage.example.com/" + path
	return &uri, nil
}

func (m *mockStorage) UploadFromReader(_ context.Context, objectName string, reader io.Reader) (*string, error) {
	if m.err != nil {
		return nil, m.err
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	m.objects[objectName] = data
	uri := "https://storage.example.com/" + objectName
	return &uri, nil
}

func (m *mockStorage) DownloadToPath(_ context.Context, _, _ string) error {
	return m.err
}

func (m *mockStorage) Download(_ context.Context, objectName string, writer io.Writer) error {
	if m.err != nil {
		return m.err
	}
	data, ok := m.objects[objectName]
	if !ok {
		return errors.New("object not found")
	}
	_, err := writer.Write(data)
	return err
}

func (m *mockStorage) Delete(_ context.Context, objectName string) error {
	if m.err != nil {
		return m.err
	}
	if _, ok := m.objects[objectName]; !ok {
		return errors.New("object not found")
	}
	delete(m.objects, objectName)
	return nil
}

func (m *mockStorage) List(_ context.Context, prefix string) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	var names []string
	for name := range m.objects {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	return names, nil
}

func (m *mockStorage) Exists(_ context.Context, objectName string) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	_, ok := m.objects[objectName]
	return ok, nil
}

func TestUploadFromReaderAndDownload(t *testing.T) {
	var store Client = newMockStorage()
	ctx := context.Background()

	content := "file-content-here"
	uri, err := store.UploadFromReader(ctx, "docs/readme.txt", strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if uri == nil || *uri == "" {
		t.Fatal("expected non-empty URI")
	}

	var buf bytes.Buffer
	if err := store.Download(ctx, "docs/readme.txt", &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != content {
		t.Fatalf("expected %q, got %q", content, buf.String())
	}
}

func TestUploadByPath(t *testing.T) {
	var store Client = newMockStorage()
	uri, err := store.UploadByPath(context.Background(), "/tmp/photo.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if uri == nil || !strings.Contains(*uri, "photo.jpg") {
		t.Fatal("expected URI containing filename")
	}
}

func TestDownloadNotFound(t *testing.T) {
	var store Client = newMockStorage()
	var buf bytes.Buffer
	err := store.Download(context.Background(), "missing.txt", &buf)
	if err == nil {
		t.Fatal("expected error for missing object")
	}
}

func TestDeleteObject(t *testing.T) {
	store := newMockStorage()
	var s Client = store
	ctx := context.Background()

	_, _ = s.UploadFromReader(ctx, "to-delete.txt", strings.NewReader("data"))

	if err := s.Delete(ctx, "to-delete.txt"); err != nil {
		t.Fatal(err)
	}
	exists, _ := s.Exists(ctx, "to-delete.txt")
	if exists {
		t.Fatal("expected object to be deleted")
	}
}

func TestDeleteNotFound(t *testing.T) {
	var store Client = newMockStorage()
	err := store.Delete(context.Background(), "ghost.txt")
	if err == nil {
		t.Fatal("expected error deleting nonexistent object")
	}
}

func TestListObjects(t *testing.T) {
	store := newMockStorage()
	var s Client = store
	ctx := context.Background()

	_, _ = s.UploadFromReader(ctx, "images/a.png", strings.NewReader("a"))
	_, _ = s.UploadFromReader(ctx, "images/b.png", strings.NewReader("b"))
	_, _ = s.UploadFromReader(ctx, "docs/c.pdf", strings.NewReader("c"))

	names, err := s.List(ctx, "images/")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(names))
	}

	all, err := s.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(all))
	}
}

func TestExistsObject(t *testing.T) {
	store := newMockStorage()
	var s Client = store
	ctx := context.Background()

	exists, err := s.Exists(ctx, "nope.txt")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("expected object to not exist")
	}

	_, _ = s.UploadFromReader(ctx, "yes.txt", strings.NewReader("hi"))
	exists, err = s.Exists(ctx, "yes.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("expected object to exist")
	}
}

func TestStorageError(t *testing.T) {
	expectedErr := errors.New("storage unavailable")
	store := &mockStorage{objects: make(map[string][]byte), err: expectedErr}
	var s Client = store
	ctx := context.Background()

	_, err := s.UploadFromReader(ctx, "fail.txt", strings.NewReader("data"))
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected storage error, got %v", err)
	}

	err = s.Download(ctx, "fail.txt", &bytes.Buffer{})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected storage error, got %v", err)
	}

	err = s.Delete(ctx, "fail.txt")
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected storage error, got %v", err)
	}

	_, err = s.List(ctx, "")
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected storage error, got %v", err)
	}

	_, err = s.Exists(ctx, "fail.txt")
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected storage error, got %v", err)
	}
}

package cloud_redis

import (
	"context"
	"fmt"
	"testing"
	"time"
)

var con Redis

func init() {
	host := "127.0.0.1"
	port := "6379"
	con, _ = New(host, "", port, 30*time.Millisecond)
}

func TestHSetData(t *testing.T) {
	err := con.HSetData(
		context.Background(),
		"test",
		"hello",
		"world",
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestHSetDataExpire(t *testing.T) {
	err := con.HSetData(
		context.Background(),
		"test",
		"willexpire", "hahah",
		1*time.Second) // in seconds
	if err != nil {
		t.Fatal(err)
	}
}

func TestHGet(t *testing.T) {
	err := con.HSetData(
		context.Background(),
		"test",
		"kaka", "world")
	if err != nil {
		t.Fatal(err)
	}
	data, err := con.HGetData(context.Background(), "test", "kaka")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(data)
}

func TestHGetAll(t *testing.T) {
	err := con.HSetData(
		context.Background(),
		"test",
		"kaka", "world")
	if err != nil {
		t.Fatal(err)
	}
	err = con.HSetData(
		context.Background(),
		"test",
		"kaki", "hello")
	if err != nil {
		t.Fatal(err)
	}
	data, err := con.HGetAllData(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(data)
}

func TestLock(t *testing.T) {
	if ok, err := con.Lock(context.Background(), "aesir", "1234", 24*time.Hour); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("cannot get lock")
	}
}

func TestUnlock(t *testing.T) {
	if ok, err := con.Unlock(context.Background(), "aesir", "1234"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("cannot get lock")
	}
}

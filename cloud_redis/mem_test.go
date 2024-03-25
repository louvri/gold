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
		map[string]interface{}{
			"hello": "world",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestHSetDataExpire(t *testing.T) {
	err := con.HSetData(
		context.Background(),
		"test",
		map[string]interface{}{
			"willexpire": "hahah",
		}, 5*time.Second) // in seconds
	if err != nil {
		t.Fatal(err)
	}
}

func TestHGet(t *testing.T) {
	err := con.HSetData(
		context.Background(),
		"test",
		map[string]interface{}{
			"kaka": "world",
		},
		5*time.Second)
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
		map[string]interface{}{
			"kaka": "world",
		},
		5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	err = con.HSetData(
		context.Background(),
		"test",
		map[string]interface{}{
			"kaki": "hello",
		},
		5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	data, err := con.HGetAllData(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(data)
}

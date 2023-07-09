package cloud_redis

import (
	"fmt"
	"testing"
)

var con CloudRedis

func init() {
	host := "127.0.0.1"
	port := "6379"
	con = New(host, "", port, 30)
}

func TestHSetData(t *testing.T) {
	err := con.HSetData("test", "hello", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestHSetDataExpire(t *testing.T) {
	err := con.HSetData("test", "willexpire", []byte("hahah"), 5) // in seconds
	if err != nil {
		t.Fatal(err)
	}
}

func TestHGet(t *testing.T) {
	err := con.HSetData("test", "kaka", []byte("world"), 5)
	if err != nil {
		t.Fatal(err)
	}
	data, err := con.HGetData("test", "kaka")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(data)
}

func TestHGetAll(t *testing.T) {
	err := con.HSetData("test", "kaka", []byte("world"), 5)
	if err != nil {
		t.Fatal(err)
	}
	err = con.HSetData("test", "kaki", []byte("hello"), 5)
	if err != nil {
		t.Fatal(err)
	}
	data, err := con.HGetAllData("test")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(data)
}

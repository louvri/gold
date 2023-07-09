package election

import (
	cloudRedis "github.com/louvri/gold/cloud_redis"
	"testing"
)

var con cloudRedis.CloudRedis

func init() {
	host := "127.0.0.1"
	port := "6379"
	con = cloudRedis.New(host, "", port, 60)
}

func TestGovernMechanism(t *testing.T) {
	if err := con.Delete("election:test"); err != nil {
		panic(err)
	}
	domain := "election:test"
	id1 := "1"
	leader1 := NewLeader(con)
	if err := leader1.Register(domain, id1); err != nil {
		t.Log(err)
		t.Fail()
	}
	id2 := "2"
	leader2 := NewLeader(con)
	if err := leader2.Register(domain, id2); err != nil {
		t.Log(err)
		t.Fail()
	}
	printToScreen := make([]string, 0)

	if err := leader1.Govern(); err == nil {
		printToScreen = append(printToScreen, "Hello World")
	} else {
		t.Log(err.Error())
	}

	if err := leader2.Govern(); err == nil {
		printToScreen = append(printToScreen, "Bye World")
	} else {
		t.Log(err.Error())
	}
	if len(printToScreen) != 1 {
		t.Log("locking failed", printToScreen)
		t.Fail()
	}
	printToScreen = make([]string, 0)
	if err := leader1.StepDown(); err != nil {
		t.Log(err.Error())
		t.Fail()
	}
	if err := leader2.Govern(); err == nil {
		printToScreen = append(printToScreen, "Bye World")
	} else {
		t.Log(err.Error())
	}
	if len(printToScreen) != 1 {
		t.Log("step-down failed", printToScreen)
		t.Fail()
	}
}

package election

import (
	"encoding/json"
	"errors"
	"fmt"
	cloudRedis "github.com/louvri/gold/cloud_redis"
	"strings"
)

type Leader interface {
	Register(domain, name string) error
	Govern() error
	StepDown() error
	Retire() error
}

func NewLeader(con cloudRedis.CloudRedis) Leader {
	return &leader{
		con: con,
	}
}

type leader struct {
	con    cloudRedis.CloudRedis
	domain string
	id     string
}

func (e *leader) Register(domain, id string) error {
	e.domain = domain
	e.id = id
	request := make(map[string]string)
	request["node"] = e.id
	request["status"] = "candidate"
	marshalled, err := json.Marshal(request)
	if err != nil {
		return err
	}
	return e.con.HSetData(fmt.Sprintf("%s:nodes", e.domain), e.id, marshalled)
}

func (e *leader) Govern() error {
	var self map[string]string
	var elected map[string]string
	tmp, err := e.con.HGetData(fmt.Sprintf("%s:nodes", e.domain), e.id)
	if err != nil {
		toString := string(tmp)
		isNil := strings.Contains(toString, "redigo: nil")
		if !isNil {
			return err
		} else {
			self = make(map[string]string)
			self["node"] = e.id
			self["status"] = "candidate"
			if marshalled, err := json.Marshal(self); err != nil {
				return err
			} else {
				if err := e.con.HSetData(fmt.Sprintf("%s:nodes", e.domain), e.id, marshalled); err != nil {
					return err
				}
			}

		}
		return err
	} else if len(tmp) > 0 {
		if err := json.Unmarshal(tmp, &self); err != nil {
			return err
		}
	}
	tmp, err = e.con.GetData(fmt.Sprintf("%s:elected_node", e.domain))
	if err != nil {
		isNil := strings.Contains(err.Error(), "redigo: nil")
		if !isNil {
			return err
		}

	} else if len(tmp) > 0 {
		if err = json.Unmarshal(tmp, &elected); err != nil {
			return err
		}
	}
	if elected != nil && self["node"] != elected["node"] {
		return errors.New("node not eligible to govern")
	}
	//election
	if self["status"] == "candidate" {
		self["status"] = "leader"
		bytes, err := json.Marshal(self)
		if err != nil {
			return err
		}
		if err = e.con.SetData(fmt.Sprintf("%s:elected_node", e.domain), bytes); err != nil {
			return err
		}
		if err = e.con.HSetData(fmt.Sprintf("%s:nodes", e.domain), e.id, bytes); err != nil {
			return err
		}
	} else if self["status"] == "timeoff" {
		if self["cycle"] == "1" {
			self["cycle"] = "2"
		} else if self["cycle"] == "2" {
			self["status"] = "candidate"
			self["cycle"] = ""
		}
		bytes, err := json.Marshal(self)
		if err != nil {
			return err
		}
		if err = e.con.HSetData(fmt.Sprintf("%s:nodes", e.domain), e.id, bytes); err != nil {
			return err
		}
	}
	if self["status"] != "leader" {
		return errors.New("node not eligible to govern")
	}
	return nil
}

func (e *leader) StepDown() error {
	request := make(map[string]string)
	request["node"] = e.id
	request["status"] = "timeoff"
	request["cycle"] = "1"
	marshalled, err := json.Marshal(request)
	if err != nil {
		return err
	}
	if err := e.con.HSetData(fmt.Sprintf("%s:nodes", e.domain), e.id, marshalled); err != nil {
		return err
	}
	if err := e.con.SetData(fmt.Sprintf("%s:elected_node", e.domain), []byte("")); err != nil {
		return err
	}
	return nil
}

func (e *leader) Retire() error {
	if err := e.con.SetData(fmt.Sprintf("%s:elected_node", e.domain), []byte("")); err != nil {
		return err
	}
	if err := e.con.HDelete(fmt.Sprintf("%s:nodes", e.domain), e.id); err != nil {
		return err
	}
	return nil
}

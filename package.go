package redismq

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type Package struct {
	Payload    string
	CreatedAt  time.Time
	Queue      *Queue      `json:"-"`
	Consumer   *Consumer   `json:"-"`
	Collection *[]*Package `json:"-"`
	Acked      bool        `json:"-"`
	//TODO add Headers or smth. when needed
	//wellle suggested error headers for failed packages
}

func UnmarshalPackage(input string, queue *Queue, consumer *Consumer) (*Package, error) {
	p := &Package{Queue: queue, Consumer: consumer, Acked: false}
	err := json.Unmarshal([]byte(input), p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (self *Package) GetString() string {
	//Switch encoding from JSON to GOB to see delta perf.
	json, err := json.Marshal(self)
	if err != nil {
		log.Printf(" Queue failed to marshal content %s [%s]", self, err.Error())
		//TODO build sensible error handling
		return ""
	}
	return string(json)
}

func (self *Package) Index() int {
	if self.Collection == nil {
		return 0
	}
	var i int
	for i = range *self.Collection {
		if (*self.Collection)[i] == self {
			break
		}
	}
	return i
}

//TODO write in lua
func (self *Package) MultiAck() (err error) {
	if self.Collection == nil {
		return fmt.Errorf("cannot MultiAck single package")
	}
	for i := 0; i <= self.Index(); i++ {
		var p *Package
		p = (*self.Collection)[i]
		//if the package has already been acked just skip
		if p.Acked == true {
			continue
		}

		err = self.Consumer.AckPackage(p)
		if err != nil {
			break
		}
		p.Acked = true
	}
	return
}

func (self *Package) Ack() error {
	if self.Collection != nil {
		return fmt.Errorf("cannot Ack package in multi package answer")
	}
	err := self.Consumer.AckPackage(self)
	return err
}

func (self *Package) Reject(requeue bool) error {
	if self.Collection != nil && (*self.Collection)[self.Index()-1].Acked == false {
		return fmt.Errorf("cannot reject package while unacked package before it")
	}

	if !requeue {
		err := self.Consumer.FailPackage(self)
		return err
	}
	err := self.Consumer.RequeuePackage(self)
	return err
}

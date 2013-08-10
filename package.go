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
	Consumer   string      `json:"-"`
	Collection *[]*Package `json:"-"`
	Acked      bool        `json:"-"`
	//TODO add Headers or smth. when needed
	//wellle suggested error headers for failed packages
}

func UnmarshalPackage(input string, queue *Queue, consumer string) (*Package, error) {
	p := &Package{Queue: queue, Consumer: consumer, Acked: false}
	err := json.Unmarshal([]byte(input), p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Package) GetString() string {
	//Switch encoding from JSON to GOB to see delta perf.
	json, err := json.Marshal(p)
	if err != nil {
		log.Printf(" Queue failed to marshal content %s [%s]", p, err.Error())
		//TODO build sensible error handling
		return ""
	}
	return string(json)
}

func (p *Package) Index() int {
	if p.Collection == nil {
		return 0
	}
	var i int
	for i = range *p.Collection {
		if (*p.Collection)[i] == p {
			break
		}
	}
	return i
}

func (p *Package) MutliAck() (err error) {
	if p.Collection == nil {
		return fmt.Errorf("cannot MultiAck single package")
	}
	for i := 0; i <= p.Index(); i++ {
		var p2 *Package
		p2 = (*p.Collection)[i]
		//if the package has already been acked just skip
		if p2.Acked == true {
			continue
		}

		err = p2.Queue.AckPackage(p2)
		if err != nil {
			break
		}
		p2.Acked = true
	}
	return
}

func (p *Package) Ack() error {
	if p.Collection != nil {
		return fmt.Errorf("cannot Ack package in multi package answer")
	}
	err := p.Queue.AckPackage(p)
	return err
}

func (p *Package) Reject(requeue bool) error {
	if p.Collection != nil && (*p.Collection)[p.Index()-1].Acked == false {
		return fmt.Errorf("cannot reject package while unacked package before it")
	}

	if !requeue {
		err := p.Queue.FailPackage(p)
		return err
	}
	err := p.Queue.RequeuePackage(p)
	return err
}

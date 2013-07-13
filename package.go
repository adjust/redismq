package rqueue

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
	//TODO add Headers or smth. when needed
}

func UnmarshalPackage(input string, queue *Queue, consumer string) (*Package, error) {
	p := &Package{Queue: queue, Consumer: consumer}
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

func (p *Package) Ack() (err error) {
	for i := 0; i <= p.Index(); i++ {
		var p2 *Package
		if i == 0 {
			p2 = p
		} else {
			p2 = (*p.Collection)[i]
		}
		err = p2.Queue.AckPackage(p2)
	}
	return
}

func (p *Package) Reject(requeue bool) error {
	if p.Index() != 0 {
		return fmt.Errorf("cannot reject package while unacked package before it")
	}

	if !requeue {
		err := p.Queue.FailPackage(p)
		return err
	}
	err := p.Queue.RequeuePackage(p)
	return err
}

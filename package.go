package rqueue

import (
	"encoding/json"
	//"fmt"
	"log"
	"time"
)

type Package struct {
	Payload   string
	CreatedAt time.Time
	Queue     *Queue `json:"-"`
	Consumer  string `json:"-"`
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

func (p *Package) Ack() error {
	err := p.Queue.AckPackage(p)
	return err
}

func (p *Package) Reject(requeue bool) error {
	if !requeue {
		err := p.Queue.FailPackage(p)
		return err
	}
	err := p.Queue.RequeuePackage(p)
	return err
}

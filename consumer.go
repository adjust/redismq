package redismq

import (
	"github.com/adeven/redis"
)

type Consumer struct {
	Name  string
	Queue *Queue
}

func (self *Queue) AddConsumer(name string) (c *Consumer, err error) {
	c = &Consumer{Name: name, Queue: self}
	//check uniqueness and start heartbeat
	self.Consumers = append(self.Consumers, c)
	return c, nil
}

func (self *Consumer) HasUnacked() bool {
	if self.GetUnackedLength() != 0 {
		return true
	}
	return false
}

func (self *Consumer) WorkingName() string {
	return self.Queue.ConsumerWorkingPrefix() + "::" + self.Name
}
func (self *Consumer) WorkingCounterName() string {
	return self.WorkingName() + "::counter"
}
func (self *Consumer) AckCounterName() string {
	return self.Queue.ConsumerAckPrefix() + "::" + self.Name + "::counter"
}

func (self *Consumer) parseRedisAnswer(answer *redis.StringReq) (*Package, error) {
	if answer.Err() != nil {
		return nil, answer.Err()
	}
	p, err := UnmarshalPackage(answer.Val(), self.Queue, self)
	if err != nil {
		return nil, err
	}
	return p, nil
}

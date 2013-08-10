package redismq

import (
	"fmt"
)

func (self *Consumer) Get() (*Package, error) {
	if self.HasUnacked() {
		return nil, fmt.Errorf("unacked Packages found!")
	}
	return self.unsafeGet()
}

func (self *Consumer) unsafeGet() (*Package, error) {
	answer := self.Queue.redisClient.BRPopLPush(self.Queue.InputName(), self.WorkingName(), 0)
	self.Queue.redisClient.Incr(self.WorkingCounterName())
	return self.parseRedisAnswer(answer)
}

//Implement in a faster way using transactions
func (self *Consumer) MultiGet(length int) ([]*Package, error) {
	var collection []*Package
	if self.HasUnacked() {
		return nil, fmt.Errorf("unacked Packages found!")
	}
	for i := 0; i < length; i++ {
		p, err := self.unsafeGet()
		if err != nil {
			return nil, err
		}
		p.Collection = &collection
		collection = append(collection, p)
	}
	return collection, nil
}

func (self *Consumer) GetUnacked() (*Package, error) {
	if !self.HasUnacked() {
		return nil, fmt.Errorf("no unacked Packages found!")
	}
	answer := self.Queue.redisClient.LIndex(self.WorkingName(), -1)
	return self.parseRedisAnswer(answer)
}

func (self *Consumer) GetFailed() (*Package, error) {
	answer := self.Queue.redisClient.RPopLPush(self.Queue.FailedName(), self.WorkingName())
	self.Queue.redisClient.Incr(self.WorkingCounterName())
	return self.parseRedisAnswer(answer)
}

func (self *Consumer) AckPackage(p *Package) error {
	answer := self.Queue.redisClient.RPop(self.WorkingName())
	self.Queue.redisClient.Incr(self.AckCounterName())
	return answer.Err()
}

func (self *Consumer) RequeuePackage(p *Package) error {
	answer := self.Queue.redisClient.RPopLPush(self.WorkingName(), self.Queue.InputName())
	self.Queue.redisClient.Incr(self.Queue.InputCounterName())
	return answer.Err()
}

func (self *Consumer) FailPackage(p *Package) error {
	answer := self.Queue.redisClient.RPopLPush(self.WorkingName(), self.Queue.FailedName())
	self.Queue.redisClient.Incr(self.Queue.FailedCounterName())
	return answer.Err()
}

func (self *Consumer) ResetWorking() error {
	answer := self.Queue.redisClient.Del(self.WorkingName())
	return answer.Err()
}

func (self *Consumer) GetUnackedLength() int64 {
	l := self.Queue.redisClient.LLen(self.WorkingName())
	return l.Val()
}

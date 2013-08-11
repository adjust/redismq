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
	answer := self.GetQueue().redisClient.BRPopLPush(self.GetQueue().InputName(), self.WorkingName(), 0)
	self.GetQueue().redisClient.Incr(self.WorkingCounterName())
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
	answer := self.GetQueue().redisClient.LIndex(self.WorkingName(), -1)
	return self.parseRedisAnswer(answer)
}

func (self *Consumer) GetFailed() (*Package, error) {
	answer := self.GetQueue().redisClient.RPopLPush(self.GetQueue().FailedName(), self.WorkingName())
	self.GetQueue().redisClient.Incr(self.WorkingCounterName())
	return self.parseRedisAnswer(answer)
}

func (self *Consumer) AckPackage(p *Package) error {
	answer := self.GetQueue().redisClient.RPop(self.WorkingName())
	self.GetQueue().redisClient.Incr(self.AckCounterName())
	return answer.Err()
}

func (self *Consumer) RequeuePackage(p *Package) error {
	answer := self.GetQueue().redisClient.RPopLPush(self.WorkingName(), self.GetQueue().InputName())
	self.GetQueue().redisClient.Incr(self.GetQueue().InputCounterName())
	return answer.Err()
}

func (self *Consumer) FailPackage(p *Package) error {
	answer := self.GetQueue().redisClient.RPopLPush(self.WorkingName(), self.GetQueue().FailedName())
	self.GetQueue().redisClient.Incr(self.GetQueue().FailedCounterName())
	return answer.Err()
}

func (self *Consumer) ResetWorking() error {
	answer := self.GetQueue().redisClient.Del(self.WorkingName())
	return answer.Err()
}

package redismq

import (
	"fmt"
	"strconv"
	"time"
)

func (self *Queue) Put(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: self}
	answer := self.redisClient.LPush(self.InputName(), p.GetString())
	self.redisClient.Incr(self.InputCounterName())
	return answer.Err()
}

func (self *Queue) Get(consumer string) (*Package, error) {
	if self.HasUnacked(consumer) {
		return nil, fmt.Errorf("unacked Packages found!")
	}
	return self.unsafeGet(consumer)
}

func (self *Queue) unsafeGet(consumer string) (*Package, error) {
	answer := self.redisClient.BRPopLPush(self.InputName(), self.WorkingName(consumer), 0)
	self.redisClient.Incr(self.WorkingCounterName(consumer))
	return self.parseRedisAnswer(answer, consumer)
}

func (self *Queue) MultiGet(consumer string, length int) ([]*Package, error) {
	var collection []*Package
	if self.HasUnacked(consumer) {
		return nil, fmt.Errorf("unacked Packages found!")
	}
	for i := 0; i < length; i++ {
		p, err := self.unsafeGet(consumer)
		if err != nil {
			return nil, err
		}
		p.Collection = &collection
		collection = append(collection, p)
	}
	return collection, nil
}

func (self *Queue) GetUnacked(consumer string) (*Package, error) {
	if !self.HasUnacked(consumer) {
		return nil, fmt.Errorf("no unacked Packages found!")
	}
	answer := self.redisClient.LIndex(self.WorkingName(consumer), -1)
	return self.parseRedisAnswer(answer, consumer)
}

func (self *Queue) GetFailed(consumer string) (*Package, error) {
	answer := self.redisClient.RPopLPush(self.FailedName(), self.WorkingName(consumer))
	self.redisClient.Incr(self.WorkingCounterName(consumer))
	return self.parseRedisAnswer(answer, consumer)
}

//TODO implement this in lua
func (self *Queue) RequeueFailed() error {
	l := self.GetFailedLength()
	for l > 0 {
		answer := self.redisClient.RPopLPush(self.FailedName(), self.InputName())
		if answer.Err() != nil {
			return answer.Err()
		}
		self.redisClient.Incr(self.InputCounterName())
		l--
	}
	return nil
}

func (self *Queue) ResetInput() error {
	answer := self.redisClient.Del(self.InputName())
	return answer.Err()
}

func (self *Queue) ResetFailed() error {
	answer := self.redisClient.Del(self.FailedName())
	return answer.Err()
}

func (self *Queue) ResetWorking(consumer string) error {
	answer := self.redisClient.Del(self.WorkingName(consumer))
	return answer.Err()
}

func (self *Queue) AckPackage(p *Package) error {
	answer := self.redisClient.RPop(self.WorkingName(p.Consumer))
	self.redisClient.Incr(self.AckCounterName(p.Consumer))
	return answer.Err()
}

func (self *Queue) RequeuePackage(p *Package) error {
	answer := self.redisClient.RPopLPush(self.WorkingName(p.Consumer), self.InputName())
	self.redisClient.Incr(self.InputCounterName())
	return answer.Err()
}

func (self *Queue) FailPackage(p *Package) error {
	answer := self.redisClient.RPopLPush(self.WorkingName(p.Consumer), self.FailedName())
	self.redisClient.Incr(self.FailedCounterName())
	return answer.Err()
}

//Statistics
func (self *Queue) GetInputLength() int64 {
	l := self.redisClient.LLen(self.InputName())
	return l.Val()
}

func (self *Queue) GetFailedLength() int64 {
	l := self.redisClient.LLen(self.FailedName())
	return l.Val()
}

func (self *Queue) GetUnackedLength(consumer string) int64 {
	l := self.redisClient.LLen(self.WorkingName(consumer))
	return l.Val()
}

func (self *Queue) GetInputRate() int64 {
	val, err := strconv.ParseInt(self.redisClient.GetSet(self.InputCounterName(), "0").Val(), 10, 64)
	if err != nil {
		return 0
	}
	return val
}

func (self *Queue) GetFailedRate() int64 {
	val, err := strconv.ParseInt(self.redisClient.GetSet(self.FailedCounterName(), "0").Val(), 10, 64)
	if err != nil {
		return 0
	}
	return val
}

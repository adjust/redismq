package redismq

import (
	"github.com/adeven/redis"
	"strconv"
	"time"
)

func (self *Queue) Put(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: self}
	_, err := self.redisClient.Pipelined(func(c *redis.PipelineClient) {
		c.LPush(self.InputName(), p.GetString())
		c.Incr(self.InputCounterName())
	})
	return err
}

// TODO implement this in lua
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

// Statistics
func (self *Queue) GetInputLength() int64 {
	l := self.redisClient.LLen(self.InputName())
	return l.Val()
}

func (self *Queue) GetFailedLength() int64 {
	l := self.redisClient.LLen(self.FailedName())
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

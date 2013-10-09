package redismq

import (
	"fmt"
	"github.com/adeven/redis"
	"time"
)

func (self *Consumer) Get() (*Package, error) {
	if self.HasUnacked() {
		return nil, fmt.Errorf("unacked Packages found!")
	}
	return self.unsafeGet()
}

func (self *Consumer) unsafeGet() (*Package, error) {
	var answer *redis.StringReq

	self.GetQueue().redisClient.Pipelined(func(c *redis.PipelineClient) {
		answer = c.BRPopLPush(self.GetQueue().InputName(), self.WorkingName(), 0)
		c.Incr(self.WorkingCounterName())
	})

	return self.parseRedisAnswer(answer)
}

//Implement in a faster way using transactions
func (self *Consumer) MultiGet(length int) ([]*Package, error) {
	var collection []*Package
	if self.HasUnacked() {
		return nil, fmt.Errorf("unacked Packages found!")
	}

	//TODO maybe use transactions for rollback in case of errors?
	reqs, err := self.GetQueue().redisClient.Pipelined(func(c *redis.PipelineClient) {
		c.BRPopLPush(self.GetQueue().InputName(), self.WorkingName(), 0)
		for i := 1; i < length; i++ {
			c.RPopLPush(self.GetQueue().InputName(), self.WorkingName())
		}

	})
	if err != nil {
		return nil, err
	}

	for _, answer := range reqs {
		switch answer := answer.(type) {
		default:
			return nil, err
		case *redis.StringReq:
			if answer.Val() == "" {
				continue
			}
			p, err := self.parseRedisAnswer(answer)
			if err != nil {
				return nil, err
			}
			p.Collection = &collection
			collection = append(collection, p)
		}
	}
	self.GetQueue().redisClient.IncrBy(self.WorkingCounterName(), int64(length))

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
	var answer *redis.StringReq

	self.GetQueue().redisClient.Pipelined(func(c *redis.PipelineClient) {
		answer = c.RPopLPush(self.GetQueue().FailedName(), self.WorkingName())
		c.Incr(self.WorkingCounterName())
	})

	return self.parseRedisAnswer(answer)
}

func (self *Consumer) AckPackage(p *Package) error {
	_, err := self.GetQueue().redisClient.Pipelined(func(c *redis.PipelineClient) {
		c.RPop(self.WorkingName())
		c.Incr(self.AckCounterName())
	})
	return err
}

func (self *Consumer) RequeuePackage(p *Package) error {
	_, err := self.GetQueue().redisClient.Pipelined(func(c *redis.PipelineClient) {
		c.RPopLPush(self.WorkingName(), self.GetQueue().InputName())
		c.Incr(self.GetQueue().InputCounterName())
	})
	return err
}

func (self *Consumer) FailPackage(p *Package) error {
	_, err := self.GetQueue().redisClient.Pipelined(func(c *redis.PipelineClient) {
		c.RPopLPush(self.WorkingName(), self.GetQueue().FailedName())
		c.Incr(self.GetQueue().FailedCounterName())
	})
	return err
}

func (self *Consumer) ResetWorking() error {
	answer := self.GetQueue().redisClient.Del(self.WorkingName())
	return answer.Err()
}

func (self *Consumer) startHeartbeat() {
	go func() {
		for {
			self.Queue.redisClient.SetEx(self.HeartbeatName(), 1, "ping")
			time.Sleep(500 * time.Millisecond)
		}
	}()
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

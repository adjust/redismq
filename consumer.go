package redismq

import (
	"fmt"
	"github.com/adeven/redis"
	"time"
)

// Consumer are used for reading from queues
type Consumer struct {
	*broker
}

// AddConsumer returns a conumser that can write from the queue
func (queue *Queue) AddConsumer(name string) (c *Consumer, err error) {
	c = &Consumer{&broker{Name: name, Queue: queue}}
	//check uniqueness and start heartbeat
	added := queue.redisClient.SAdd(queue.workerKey(), name).Val()
	if added == 0 {
		val := queue.redisClient.Get(c.heartbeatName()).Val()
		if val == "ping" {
			return nil, fmt.Errorf("consumer with this name is already active")
		}
	}
	c.startHeartbeat()
	return c, nil
}

// Get returns a single package from the queue
func (consumer *Consumer) Get() (*Package, error) {
	if consumer.HasUnacked() {
		return nil, fmt.Errorf("unacked Packages found")
	}
	return consumer.unsafeGet()
}

// MultiGet returns an array of packages from the queue
func (consumer *Consumer) MultiGet(length int) ([]*Package, error) {
	var collection []*Package
	if consumer.HasUnacked() {
		return nil, fmt.Errorf("unacked Packages found")
	}

	// TODO maybe use transactions for rollback in case of errors?
	reqs, err := consumer.Queue.redisClient.Pipelined(func(c *redis.PipelineClient) {
		c.BRPopLPush(consumer.Queue.inputName(), consumer.WorkingName(), 0)
		for i := 1; i < length; i++ {
			c.RPopLPush(consumer.Queue.inputName(), consumer.WorkingName())
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
			p, err := consumer.parseRedisAnswer(answer)
			if err != nil {
				return nil, err
			}
			p.Collection = &collection
			collection = append(collection, p)
		}
	}
	consumer.Queue.redisClient.IncrBy(consumer.WorkingCounterName(), int64(length))

	return collection, nil
}

// GetUnacked returns a single packages from the working queue of this consumer
func (consumer *Consumer) GetUnacked() (*Package, error) {
	if !consumer.HasUnacked() {
		return nil, fmt.Errorf("no unacked Packages found")
	}
	answer := consumer.Queue.redisClient.LIndex(consumer.WorkingName(), -1)
	return consumer.parseRedisAnswer(answer)
}

// GetFailed returns a single packages from the failed queue of this consumer
func (consumer *Consumer) GetFailed() (*Package, error) {
	var answer *redis.StringReq

	consumer.Queue.redisClient.Pipelined(func(c *redis.PipelineClient) {
		answer = c.RPopLPush(consumer.Queue.failedName(), consumer.WorkingName())
		c.Incr(consumer.WorkingCounterName())
	})

	return consumer.parseRedisAnswer(answer)
}

// ResetWorking deletes! all messages in the working queue of this consumer
func (consumer *Consumer) ResetWorking() error {
	answer := consumer.Queue.redisClient.Del(consumer.WorkingName())
	return answer.Err()
}

// RequeueWorking requeues all packages from working to input
func (consumer *Consumer) RequeueWorking() {
	for consumer.HasUnacked() {
		p := consumer.GetUnacked()
		p.Reject(true)
	}
}

func (consumer *Consumer) ackPackage(p *Package) error {
	_, err := consumer.Queue.redisClient.Pipelined(func(c *redis.PipelineClient) {
		c.RPop(consumer.WorkingName())
		c.Incr(consumer.AckCounterName())
	})
	return err
}

func (consumer *Consumer) requeuePackage(p *Package) error {
	_, err := consumer.Queue.redisClient.Pipelined(func(c *redis.PipelineClient) {
		c.RPopLPush(consumer.WorkingName(), consumer.Queue.inputName())
		c.Incr(consumer.Queue.inputCounterName())
	})
	return err
}

func (consumer *Consumer) failPackage(p *Package) error {
	_, err := consumer.Queue.redisClient.Pipelined(func(c *redis.PipelineClient) {
		c.RPopLPush(consumer.WorkingName(), consumer.Queue.failedName())
		c.Incr(consumer.Queue.failedCounterName())
	})
	return err
}

func (consumer *Consumer) startHeartbeat() {
	firstWrite := make(chan bool, 1)
	go func() {
		firstRun := true
		for {
			consumer.Queue.redisClient.SetEx(consumer.heartbeatName(), 1, "ping")
			if firstRun {
				firstWrite <- true
				firstRun = false
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
	<-firstWrite
	return
}

func (consumer *Consumer) parseRedisAnswer(answer *redis.StringReq) (*Package, error) {
	if answer.Err() != nil {
		return nil, answer.Err()
	}
	p, err := unmarshalPackage(answer.Val(), consumer.Queue, consumer)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (consumer *Consumer) heartbeatName() string {
	return consumer.WorkingName() + "::heartbeat"
}

func (consumer *Consumer) unsafeGet() (*Package, error) {
	var answer *redis.StringReq

	consumer.Queue.redisClient.Pipelined(func(c *redis.PipelineClient) {
		answer = c.BRPopLPush(consumer.Queue.inputName(), consumer.WorkingName(), 0)
		c.Incr(consumer.WorkingCounterName())
	})

	return consumer.parseRedisAnswer(answer)
}

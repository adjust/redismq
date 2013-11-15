package redismq

import (
	"fmt"
	"github.com/adeven/redis"
	"strconv"
	"time"
)

// Queue is the central element of this library.
// Packages can be put into or get from the queue.
// To read from a queue you need a consumer.
type Queue struct {
	redisClient *redis.Client
	Name        string
}

// CreateQueue return a queue that you can Put() or AddConsumer() to
// Works like SelectQueue for existing queues
func CreateQueue(redisURL, redisPassword string, redisDB int64, name string) *Queue {
	q := &Queue{Name: name}
	q.redisClient = redis.NewTCPClient(redisURL, redisPassword, redisDB)
	q.redisClient.SAdd(masterQueueKey(), name)
	return q
}

// SelectQueue returns a Queue if a queue with the name exists
func SelectQueue(redisURL, redisPassword string, redisDB int64, name string) (queue *Queue, err error) {
	queue = &Queue{Name: name}
	queue.redisClient = redis.NewTCPClient(redisURL, redisPassword, redisDB)
	answer := queue.redisClient.SIsMember(masterQueueKey(), name)
	if answer.Val() {
		return queue, nil
	}
	return nil, fmt.Errorf("queue with this name doesn't exist")
}

func masterQueueKey() string {
	return "redismq::queues"
}

func (queue *Queue) workerKey() string {
	return queue.inputName() + "::workers"
}

func (queue *Queue) inputName() string {
	return "redismq::" + queue.Name
}

func (queue *Queue) failedName() string {
	return "redismq::" + queue.Name + "::failed"
}

func (queue *Queue) inputCounterName() string {
	return queue.inputName() + "::counter"
}

func (queue *Queue) failedCounterName() string {
	return queue.failedName() + "::counter"
}

func (queue *Queue) consumerWorkingPrefix() string {
	return "redismq::" + queue.Name + "::working"
}

func (queue *Queue) consumerAckPrefix() string {
	return "redismq::" + queue.Name + "::ack"
}

// Put writes the payload into the input queue
func (queue *Queue) Put(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: queue}
	_, err := queue.redisClient.Pipelined(func(c *redis.PipelineClient) {
		c.LPush(queue.inputName(), p.getString())
		c.Incr(queue.inputCounterName())
	})
	return err
}

// RequeueFailed moves all failed packages back to the input queue
func (queue *Queue) RequeueFailed() error {
	l := queue.GetFailedLength()
	// TODO implement this in lua
	for l > 0 {
		answer := queue.redisClient.RPopLPush(queue.failedName(), queue.inputName())
		if answer.Err() != nil {
			return answer.Err()
		}
		queue.redisClient.Incr(queue.inputCounterName())
		l--
	}
	return nil
}

// ResetInput deletes all packages from the input queue
func (queue *Queue) ResetInput() error {
	answer := queue.redisClient.Del(queue.inputName())
	return answer.Err()
}

// ResetFailed deletes all packages from the failed queue
func (queue *Queue) ResetFailed() error {
	answer := queue.redisClient.Del(queue.failedName())
	return answer.Err()
}

// GetInputLength returns the number of packages in the input queue
func (queue *Queue) GetInputLength() int64 {
	l := queue.redisClient.LLen(queue.inputName())
	return l.Val()
}

// GetFailedLength returns the number of packages in the failed queue
func (queue *Queue) GetFailedLength() int64 {
	l := queue.redisClient.LLen(queue.failedName())
	return l.Val()
}

func (queue *Queue) getInputRate() int64 {
	val, err := strconv.ParseInt(queue.redisClient.GetSet(queue.inputCounterName(), "0").Val(), 10, 64)
	if err != nil {
		return 0
	}
	return val
}

func (queue *Queue) getFailedRate() int64 {
	val, err := strconv.ParseInt(queue.redisClient.GetSet(queue.failedCounterName(), "0").Val(), 10, 64)
	if err != nil {
		return 0
	}
	return val
}

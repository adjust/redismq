package rqueue

import (
	"fmt"
	"github.com/adeven/goenv"
	"github.com/adeven/redis"
	"time"
)

type Queue struct {
	redisClient *redis.Client
	name        string
}

func NewQueue(goenv *goenv.Goenv, name string) *Queue {
	q := &Queue{name: name}
	host, port, db := goenv.GetRedis()
	q.redisClient = redis.NewTCPClient(host+":"+port, "", int64(db))
	return q
}

func (queue *Queue) Put(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: queue}
	answer := queue.redisClient.LPush(InputQueueName(queue), p.GetString())
	return answer.Err()
}

func (queue *Queue) Get(consumer string) (*Package, error) {
	if queue.HasUnacked(consumer) {
		return nil, fmt.Errorf("unacked Packages found!")
	}
	answer := queue.redisClient.BRPopLPush(InputQueueName(queue), WorkingQueueName(queue, consumer), 0)
	return queue.parseRedisAnswer(answer, consumer)
}

func (queue *Queue) GetUnacked(consumer string) (*Package, error) {
	if !queue.HasUnacked(consumer) {
		return nil, fmt.Errorf("no unacked Packages found!")
	}
	answer := queue.redisClient.LIndex(WorkingQueueName(queue, consumer), -1)
	return queue.parseRedisAnswer(answer, consumer)
}

func (queue *Queue) GetFailed(consumer string) (*Package, error) {
	answer := queue.redisClient.RPopLPush(FailedQueueName(queue), WorkingQueueName(queue, consumer))
	return queue.parseRedisAnswer(answer, consumer)
}

func (queue *Queue) HasUnacked(consumer string) bool {
	if queue.UnackedLength(consumer) != 0 {
		return true
	}
	return false
}

func (queue *Queue) RequeueFailed() error {
	l := queue.FailedLength()
	for l > 0 {
		answer := queue.redisClient.RPopLPush(FailedQueueName(queue), InputQueueName(queue))
		if answer.Err() != nil {
			return answer.Err()
		}
		l--
	}
	return nil
}

func (queue *Queue) InputLength() int64 {
	l := queue.redisClient.LLen(InputQueueName(queue))
	return l.Val()
}

func (queue *Queue) FailedLength() int64 {
	l := queue.redisClient.LLen(FailedQueueName(queue))
	return l.Val()
}

func (queue *Queue) UnackedLength(consumer string) int64 {
	l := queue.redisClient.LLen(WorkingQueueName(queue, consumer))
	return l.Val()
}

func (queue *Queue) ResetInput() error {
	answer := queue.redisClient.Del(InputQueueName(queue))
	return answer.Err()
}

func (queue *Queue) ResetFailed() error {
	answer := queue.redisClient.Del(FailedQueueName(queue))
	return answer.Err()
}

func (queue *Queue) ResetWorking(consumer string) error {
	answer := queue.redisClient.Del(WorkingQueueName(queue, consumer))
	return answer.Err()
}

func (queue *Queue) AckPackage(p *Package) error {
	answer := queue.redisClient.RPop(WorkingQueueName(queue, p.Consumer))
	return answer.Err()
}

func (queue *Queue) RequeuePackage(p *Package) error {
	answer := queue.redisClient.RPopLPush(WorkingQueueName(queue, p.Consumer), InputQueueName(queue))
	return answer.Err()
}

func (queue *Queue) FailPackage(p *Package) error {
	answer := queue.redisClient.RPopLPush(WorkingQueueName(queue, p.Consumer), FailedQueueName(queue))
	return answer.Err()
}

func (queue *Queue) parseRedisAnswer(answer *redis.StringReq, consumer string) (*Package, error) {
	if answer.Err() != nil {
		return nil, answer.Err()
	}
	p, err := UnmarshalPackage(answer.Val(), queue, consumer)
	if err != nil {
		return nil, err
	}
	return p, nil
}

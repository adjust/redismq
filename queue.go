package redismq

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

func (queue *Queue) InputName() string {
	return "redismq::" + queue.name
}

func (queue *Queue) WorkingName(consumer string) string {
	return "redismq::" + queue.name + "::working::" + consumer
}

func (queue *Queue) FailedName() string {
	return "redismq::" + queue.name + "::failed"
}

func (queue *Queue) CounterName() string {
	return "redismq::" + queue.name + "::counter"
}

func (queue *Queue) Put(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: queue}
	answer := queue.redisClient.LPush(queue.InputName(), p.GetString())
	queue.redisClient.Incr(queue.CounterName())
	return answer.Err()
}

func (queue *Queue) Get(consumer string) (*Package, error) {
	if queue.HasUnacked(consumer) {
		return nil, fmt.Errorf("unacked Packages found!")
	}
	return queue.unsafeGet(consumer)
}

func (queue *Queue) unsafeGet(consumer string) (*Package, error) {
	answer := queue.redisClient.BRPopLPush(queue.InputName(), queue.WorkingName(consumer), 0)
	return queue.parseRedisAnswer(answer, consumer)
}

func (queue *Queue) MultiGet(consumer string, length int) ([]*Package, error) {
	var collection []*Package
	if queue.HasUnacked(consumer) {
		return nil, fmt.Errorf("unacked Packages found!")
	}
	for i := 0; i < length; i++ {
		p, err := queue.unsafeGet(consumer)
		if err != nil {
			return nil, err
		}
		p.Collection = &collection
		collection = append(collection, p)
	}
	return collection, nil
}

func (queue *Queue) GetUnacked(consumer string) (*Package, error) {
	if !queue.HasUnacked(consumer) {
		return nil, fmt.Errorf("no unacked Packages found!")
	}
	answer := queue.redisClient.LIndex(queue.WorkingName(consumer), -1)
	return queue.parseRedisAnswer(answer, consumer)
}

func (queue *Queue) GetFailed(consumer string) (*Package, error) {
	answer := queue.redisClient.RPopLPush(queue.FailedName(), queue.WorkingName(consumer))
	return queue.parseRedisAnswer(answer, consumer)
}

func (queue *Queue) HasUnacked(consumer string) bool {
	if queue.UnackedLength(consumer) != 0 {
		return true
	}
	return false
}

//TODO implement this in lua
func (queue *Queue) RequeueFailed() error {
	l := queue.FailedLength()
	for l > 0 {
		answer := queue.redisClient.RPopLPush(queue.FailedName(), queue.InputName())
		if answer.Err() != nil {
			return answer.Err()
		}
		l--
	}
	return nil
}

func (queue *Queue) InputLength() int64 {
	l := queue.redisClient.LLen(queue.InputName())
	return l.Val()
}

func (queue *Queue) FailedLength() int64 {
	l := queue.redisClient.LLen(queue.FailedName())
	return l.Val()
}

func (queue *Queue) UnackedLength(consumer string) int64 {
	l := queue.redisClient.LLen(queue.WorkingName(consumer))
	return l.Val()
}

func (queue *Queue) ResetInput() error {
	answer := queue.redisClient.Del(queue.InputName())
	return answer.Err()
}

func (queue *Queue) ResetFailed() error {
	answer := queue.redisClient.Del(queue.FailedName())
	return answer.Err()
}

func (queue *Queue) ResetWorking(consumer string) error {
	answer := queue.redisClient.Del(queue.WorkingName(consumer))
	return answer.Err()
}

func (queue *Queue) AckPackage(p *Package) error {
	answer := queue.redisClient.RPop(queue.WorkingName(p.Consumer))
	return answer.Err()
}

func (queue *Queue) RequeuePackage(p *Package) error {
	answer := queue.redisClient.RPopLPush(queue.WorkingName(p.Consumer), queue.InputName())
	return answer.Err()
}

func (queue *Queue) FailPackage(p *Package) error {
	answer := queue.redisClient.RPopLPush(queue.WorkingName(p.Consumer), queue.FailedName())
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

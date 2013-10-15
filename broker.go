package redismq

import (
	"strconv"
)

type broker struct {
	Name  string
	Queue *Queue
}

func (queue *Queue) getBrokers() (brokers []*broker, err error) {
	answer := queue.redisClient.SMembers(queue.workerKey())
	if answer.Err() != nil {
		return nil, err
	}
	for _, name := range answer.Val() {
		w := &broker{Name: name, Queue: queue}
		brokers = append(brokers, w)
	}
	return
}
func (broker *broker) HasUnacked() bool {
	if broker.GetUnackedLength() != 0 {
		return true
	}
	return false
}

func (broker *broker) WorkingName() string {
	return broker.Queue.consumerWorkingPrefix() + "::" + broker.Name
}
func (broker *broker) WorkingCounterName() string {
	return broker.WorkingName() + "::counter"
}
func (broker *broker) AckCounterName() string {
	return broker.Queue.consumerAckPrefix() + "::" + broker.Name + "::counter"
}

func (broker *broker) GetUnackedLength() int64 {
	l := broker.Queue.redisClient.LLen(broker.WorkingName())
	return l.Val()
}

func (broker *broker) GetAckRate() int64 {
	val, err := strconv.ParseInt(broker.Queue.redisClient.GetSet(broker.AckCounterName(), "0").Val(), 10, 64)
	if err != nil {
		return 0
	}
	return val
}

func (broker *broker) GetWorkRate() int64 {
	val, err := strconv.ParseInt(broker.Queue.redisClient.GetSet(broker.WorkingCounterName(), "0").Val(), 10, 64)
	if err != nil {
		return 0
	}
	return val
}

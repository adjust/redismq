package redismq

import (
	"encoding/json"
	"github.com/adeven/redis"
	"log"
	"time"
)

type observer struct {
	redisClient   *redis.Client `json:"-"`
	RedisURL      string        `json:"-"`
	RedisPassword string        `json:"-"`
	RedisDb       int64         `json:"-"`
	Stats         map[string]*queueStat
}

type queueStat struct {
	InputRate     int64
	WorkRate      int64
	AckRate       int64
	FailRate      int64
	InputSize     int64
	UnAckSize     int64
	FailSize      int64
	consumerStats map[string]*consumerStat
}

type consumerStat struct {
	WorkRate  int64
	AckRate   int64
	UnAckSize int64
}

func newObserver(redisURL, redisPassword string, redisDb int64) *observer {
	q := &observer{
		RedisURL:      redisURL,
		RedisPassword: redisPassword,
		RedisDb:       redisDb,
		Stats:         make(map[string]*queueStat),
	}
	q.redisClient = redis.NewTCPClient(redisURL, redisPassword, redisDb)
	return q
}

func (observer *observer) Start() {
	go func() {
		for {
			for _, queue := range observer.GetAllQueues() {
				if observer.Stats[queue.Name] == nil {
					observer.WatchQueue(queue)
				}
			}
			time.Sleep(2 * time.Second)
		}
	}()
}

func (observer *observer) GetAllQueues() (queues []*Queue) {
	answer := observer.redisClient.SMembers(masterQueueKey())
	if answer.Err() != nil {
		return
	}
	for _, name := range answer.Val() {
		queues = append(queues, NewQueue(observer.RedisURL, observer.RedisPassword, observer.RedisDb, name))
	}
	return
}

func (observer *observer) WatchQueue(queue *Queue) {
	observer.Stats[queue.Name] = &queueStat{consumerStats: make(map[string]*consumerStat)}
	go observer.Poll(queue)
}

func (observer *observer) Poll(queue *Queue) {
	for {
		observer.Stats[queue.Name].InputRate = queue.getInputRate()
		observer.Stats[queue.Name].FailRate = queue.getFailedRate()
		observer.Stats[queue.Name].InputSize = queue.GetInputLength()
		observer.Stats[queue.Name].FailSize = queue.GetFailedLength()

		observer.Stats[queue.Name].WorkRate = 0
		observer.Stats[queue.Name].AckRate = 0
		observer.Stats[queue.Name].UnAckSize = 0

		brokers, err := queue.getBrokers()
		if err != nil {
			continue //TODO handle this
		}

		for _, broker := range brokers {
			observer.Stats[queue.Name].consumerStats[broker.Name] = &consumerStat{}
			observer.Stats[queue.Name].consumerStats[broker.Name].AckRate = broker.GetAckRate()
			observer.Stats[queue.Name].consumerStats[broker.Name].WorkRate = broker.GetWorkRate()
			observer.Stats[queue.Name].consumerStats[broker.Name].UnAckSize = broker.GetUnackedLength()

			observer.Stats[queue.Name].AckRate += observer.Stats[queue.Name].consumerStats[broker.Name].AckRate
			observer.Stats[queue.Name].WorkRate += observer.Stats[queue.Name].consumerStats[broker.Name].WorkRate
			observer.Stats[queue.Name].UnAckSize += observer.Stats[queue.Name].consumerStats[broker.Name].UnAckSize
		}

		time.Sleep(1 * time.Second)
	}

}

func (observer *observer) OutputToString() string {
	json, err := json.Marshal(observer)
	if err != nil {
		log.Fatalf("ERROR MARSHALLING OVERSEER %s", err.Error())
	}
	return string(json)
}

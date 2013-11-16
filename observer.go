package redismq

import (
	"encoding/json"
	"fmt"
	"github.com/adeven/redis"
	"log"
	"strconv"
	"time"
)

// this is a very simple implementation of a statistics observer
// far more complex things can be implemented with the way stats are written
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
	InputSize     int64
	FailSize      int64
	ConsumerStats map[string]*consumerStat
}

type consumerStat struct {
	WorkRate int64
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

func (observer *observer) getAllQueues() (queues []string, err error) {
	answer := observer.redisClient.SMembers(masterQueueKey())
	return answer.Val(), answer.Err()
}

func (observer *observer) getConsumers(queue string) (consumers []string, err error) {
	answer := observer.redisClient.SMembers(queueWorkersKey(queue))
	return answer.Val(), answer.Err()
}

func (observer *observer) update() {
	queues, err := observer.getAllQueues()
	if err != nil {
		log.Fatalf("ERROR FETCHING QUEUES %s", err.Error())
	}

	for _, queue := range queues {
		observer.poll(queue)
	}
}

func (observer *observer) poll(queue string) {
	if observer.Stats[queue] == nil {
		observer.Stats[queue] = &queueStat{ConsumerStats: make(map[string]*consumerStat)}
	}
	observer.Stats[queue].InputRate = observer.fetchStat(queueInputRateKey(queue))
	observer.Stats[queue].InputSize = observer.fetchStat(queueInputSizeKey(queue))
	observer.Stats[queue].FailSize = observer.fetchStat(queueFailedSizeKey(queue))

	observer.Stats[queue].WorkRate = 0

	consumers, err := observer.getConsumers(queue)
	if err != nil {
		log.Fatalf("ERROR FETCHING CONSUMERS for %s %s", queue, err.Error())
		return
	}

	for _, consumer := range consumers {
		if observer.Stats[queue].ConsumerStats[consumer] == nil {
			observer.Stats[queue].ConsumerStats[consumer] = &consumerStat{}
		}
		observer.Stats[queue].ConsumerStats[consumer].WorkRate = observer.fetchStat(consumerWorkingRateKey(queue, consumer))
		observer.Stats[queue].WorkRate += observer.Stats[queue].ConsumerStats[consumer].WorkRate
	}
}

func (observer *observer) fetchStat(keyName string) int64 {
	now := time.Now().UTC().Unix() - 3 // we can only look for already written stats
	key := fmt.Sprintf("%s::%d", keyName, now)
	answer := observer.redisClient.Get(key)
	if answer.Err() != nil {
		return 0
	}
	i, err := strconv.ParseInt(answer.Val(), 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func (observer *observer) OutputToString() string {
	observer.update()
	json, err := json.Marshal(observer)
	if err != nil {
		log.Fatalf("ERROR MARSHALLING OVERSEER %s", err.Error())
	}
	return string(json)
}

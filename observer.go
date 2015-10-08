package redismq

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"gopkg.in/redis.v3"
)

// Observer is a very simple implementation of an statistics observer
// far more complex things could be implemented with the way stats are written
// for now it allows basic access
// to throughput rates and queue size averaged over seconds, minutes and hours
type Observer struct {
	redisClient   *redis.Client `json:"-"`
	redisHost     string        `json:"-"`
	redisPort     string        `json:"-"`
	redisPassword string        `json:"-"`
	redisDb       int64         `json:"-"`
	Stats         map[string]*QueueStat
}

// QueueStat collects data about a queue
type QueueStat struct {
	InputSizeSecond int64
	InputSizeMinute int64
	InputSizeHour   int64

	FailSizeSecond int64
	FailSizeMinute int64
	FailSizeHour   int64

	InputRateSecond int64
	InputRateMinute int64
	InputRateHour   int64

	WorkRateSecond int64
	WorkRateMinute int64
	WorkRateHour   int64

	ConsumerStats map[string]*ConsumerStat
}

// ConsumerStat collects data about a queues consumer
type ConsumerStat struct {
	WorkRateSecond int64
	WorkRateMinute int64
	WorkRateHour   int64
}

// NewObserver returns an Oberserver to monitor different statistics from redis
func NewObserver(redisHost, redisPort, redisPassword string, redisDb int64) *Observer {
	q := &Observer{
		redisHost:     redisHost,
		redisPort:     redisPort,
		redisPassword: redisPassword,
		redisDb:       redisDb,
		Stats:         make(map[string]*QueueStat),
	}
	q.redisClient = redis.NewClient(&redis.Options{
		Addr:     redisHost + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDb,
	})
	return q
}

// UpdateAllStats fetches stats for all queues and all their consumers
func (observer *Observer) UpdateAllStats() {
	queues, err := observer.GetAllQueues()
	if err != nil {
		log.Fatalf("ERROR FETCHING QUEUES %s", err.Error())
	}

	for _, queue := range queues {
		observer.UpdateQueueStats(queue)
	}
}

// GetAllQueues returns a list of all registed queues
func (observer *Observer) GetAllQueues() (queues []string, err error) {
	return observer.redisClient.SMembers(masterQueueKey()).Result()
}

func (observer *Observer) getConsumers(queue string) (consumers []string, err error) {
	return observer.redisClient.SMembers(queueWorkersKey(queue)).Result()
}

// UpdateQueueStats fetches stats for one specific queue and its consumers
func (observer *Observer) UpdateQueueStats(queue string) {
	queueStats := &QueueStat{ConsumerStats: make(map[string]*ConsumerStat)}

	queueStats.InputRateSecond = observer.fetchStat(queueInputRateKey(queue), 1)
	queueStats.InputSizeSecond = observer.fetchStat(queueInputSizeKey(queue), 1)
	queueStats.FailSizeSecond = observer.fetchStat(queueFailedSizeKey(queue), 1)

	queueStats.InputRateMinute = observer.fetchStat(queueInputRateKey(queue), 60)
	queueStats.InputSizeMinute = observer.fetchStat(queueInputSizeKey(queue), 60)
	queueStats.FailSizeMinute = observer.fetchStat(queueFailedSizeKey(queue), 60)

	queueStats.InputRateHour = observer.fetchStat(queueInputRateKey(queue), 3600)
	queueStats.InputSizeHour = observer.fetchStat(queueInputSizeKey(queue), 3600)
	queueStats.FailSizeHour = observer.fetchStat(queueFailedSizeKey(queue), 3600)

	queueStats.WorkRateSecond = 0
	queueStats.WorkRateMinute = 0
	queueStats.WorkRateHour = 0

	consumers, err := observer.getConsumers(queue)
	if err != nil {
		log.Fatalf("ERROR FETCHING CONSUMERS for %s %s", queue, err.Error())
		return
	}

	for _, consumer := range consumers {
		stat := &ConsumerStat{}

		stat.WorkRateSecond = observer.fetchStat(consumerWorkingRateKey(queue, consumer), 1)
		stat.WorkRateMinute = observer.fetchStat(consumerWorkingRateKey(queue, consumer), 60)
		stat.WorkRateHour = observer.fetchStat(consumerWorkingRateKey(queue, consumer), 3600)

		queueStats.WorkRateSecond += stat.WorkRateSecond
		queueStats.WorkRateMinute += stat.WorkRateMinute
		queueStats.WorkRateHour += stat.WorkRateHour

		queueStats.ConsumerStats[consumer] = stat
	}

	observer.Stats[queue] = queueStats
}

// TODO the current implementation does not handle gaps for queue size
// which appear for queues with little or no traffic
func (observer *Observer) fetchStat(keyName string, seconds int64) int64 {
	now := time.Now().UTC().Unix() - 2 // we can only look for already written stats
	keys := make([]string, 0)

	for i := int64(0); i < seconds; i++ {
		key := fmt.Sprintf("%s::%d", keyName, now)
		keys = append(keys, key)
		now--
	}
	vals, err := observer.redisClient.MGet(keys...).Result()
	if err != nil {
		return 0
	}
	nilVal := 0
	sum := int64(0)
	for _, val := range vals {
		if val == nil {
			nilVal++
			continue
		}
		num, _ := strconv.ParseInt(val.(string), 10, 64)
		sum += num
	}
	return sum / seconds
}

// ToJSON renders the whole observer as a JSON string
func (observer *Observer) ToJSON() string {
	json, err := json.Marshal(observer)
	if err != nil {
		log.Fatalf("ERROR MARSHALLING OVERSEER %s", err.Error())
	}
	return string(json)
}

package redismq

import (
	"encoding/json"
	"fmt"
	"github.com/adeven/redis"
	"log"
	"strconv"
	"time"
)

// Observer is a very simple implementation of an statistics observer
// far more complex things could be implemented with the way stats are written
// for now it allows basic access
// to throughput rates and queue size averaged over seconds, minutes and hours
type Observer struct {
	redisClient   *redis.Client `json:"-"`
	redisURL      string        `json:"-"`
	redisPassword string        `json:"-"`
	redisDb       int64         `json:"-"`
	Stats         map[string]*queueStat
}

type queueStat struct {
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

	ConsumerStats map[string]*consumerStat
}

type consumerStat struct {
	WorkRateSecond int64
	WorkRateMinute int64
	WorkRateHour   int64
}

// NewObserver returns an Oberserver to monitor different statistics from redis
func NewObserver(redisURL, redisPassword string, redisDb int64) *Observer {
	q := &Observer{
		redisURL:      redisURL,
		redisPassword: redisPassword,
		redisDb:       redisDb,
		Stats:         make(map[string]*queueStat),
	}
	q.redisClient = redis.NewTCPClient(redisURL, redisPassword, redisDb)
	return q
}

// UpdateAllStats fetches stats for all queues and all their consumers
func (observer *Observer) UpdateAllStats() {
	queues, err := observer.getAllQueues()
	if err != nil {
		log.Fatalf("ERROR FETCHING QUEUES %s", err.Error())
	}

	for _, queue := range queues {
		observer.UdpateQueueStats(queue)
	}
}

func (observer *Observer) getAllQueues() (queues []string, err error) {
	answer := observer.redisClient.SMembers(masterQueueKey())
	return answer.Val(), answer.Err()
}

func (observer *Observer) getConsumers(queue string) (consumers []string, err error) {
	answer := observer.redisClient.SMembers(queueWorkersKey(queue))
	return answer.Val(), answer.Err()
}

// UdpateQueueStats fetches stats for one specific queue and its consumers
func (observer *Observer) UdpateQueueStats(queue string) {
	if observer.Stats[queue] == nil {
		observer.Stats[queue] = &queueStat{ConsumerStats: make(map[string]*consumerStat)}
	}
	observer.Stats[queue].InputRateSecond = observer.fetchStat(queueInputRateKey(queue), 1)
	observer.Stats[queue].InputSizeSecond = observer.fetchStat(queueInputSizeKey(queue), 1)
	observer.Stats[queue].FailSizeSecond = observer.fetchStat(queueFailedSizeKey(queue), 1)

	observer.Stats[queue].InputRateMinute = observer.fetchStat(queueInputRateKey(queue), 60)
	observer.Stats[queue].InputSizeMinute = observer.fetchStat(queueInputSizeKey(queue), 60)
	observer.Stats[queue].FailSizeMinute = observer.fetchStat(queueFailedSizeKey(queue), 60)

	observer.Stats[queue].InputRateHour = observer.fetchStat(queueInputRateKey(queue), 3600)
	observer.Stats[queue].InputSizeHour = observer.fetchStat(queueInputSizeKey(queue), 3600)
	observer.Stats[queue].FailSizeHour = observer.fetchStat(queueFailedSizeKey(queue), 3600)

	observer.Stats[queue].WorkRateSecond = 0
	observer.Stats[queue].WorkRateMinute = 0
	observer.Stats[queue].WorkRateHour = 0

	consumers, err := observer.getConsumers(queue)
	if err != nil {
		log.Fatalf("ERROR FETCHING CONSUMERS for %s %s", queue, err.Error())
		return
	}

	for _, consumer := range consumers {
		stat := &consumerStat{}

		stat.WorkRateSecond = observer.fetchStat(consumerWorkingRateKey(queue, consumer), 1)
		stat.WorkRateMinute = observer.fetchStat(consumerWorkingRateKey(queue, consumer), 60)
		stat.WorkRateHour = observer.fetchStat(consumerWorkingRateKey(queue, consumer), 3600)

		observer.Stats[queue].WorkRateSecond += stat.WorkRateSecond
		observer.Stats[queue].WorkRateMinute += stat.WorkRateMinute
		observer.Stats[queue].WorkRateHour += stat.WorkRateHour

		observer.Stats[queue].ConsumerStats[consumer] = stat
	}
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
	answer := observer.redisClient.MGet(keys...)
	if answer.Err() != nil {
		return 0
	}
	nilVal := 0
	sum := int64(0)
	for _, val := range answer.Val() {
		if val == nil {
			nilVal++
			continue
		}
		num, _ := strconv.ParseInt(val.(string), 10, 64)
		sum += num
	}
	if seconds == 60 {
		log.Println(len(answer.Val()))
		log.Println(sum)
		log.Println(nilVal)
		log.Println("")
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

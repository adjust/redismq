package redismq

import (
	"fmt"
	"github.com/adeven/redis"
	"strconv"
	"strings"
	"time"
)

// Queue is the central element of this library.
// Packages can be put into or get from the queue.
// To read from a queue you need a consumer.
type Queue struct {
	redisClient *redis.Client
	Name        string
	statsCache  map[int64]map[string]int64
	statsChan   chan (*dataPoint)
}

type dataPoint struct {
	name  string
	value int64
	incr  bool
	unix  int64
}

// CreateQueue return a queue that you can Put() or AddConsumer() to
// Works like SelectQueue for existing queues
func CreateQueue(redisURL, redisPassword string, redisDB int64, name string) *Queue {
	q := &Queue{Name: name}
	q.redisClient = redis.NewTCPClient(redisURL, redisPassword, redisDB)
	q.redisClient.SAdd(masterQueueKey(), name)
	q.startStatsWriter()
	return q
}

// SelectQueue returns a Queue if a queue with the name exists
func SelectQueue(redisURL, redisPassword string, redisDB int64, name string) (queue *Queue, err error) {
	queue = &Queue{Name: name}
	queue.redisClient = redis.NewTCPClient(redisURL, redisPassword, redisDB)
	answer := queue.redisClient.SIsMember(masterQueueKey(), name)
	if answer.Val() == false {
		return nil, fmt.Errorf("queue with this name doesn't exist")
	}
	queue.startStatsWriter()
	return queue, nil
}

// Put writes the payload into the input queue
func (queue *Queue) Put(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: queue}
	answer := queue.redisClient.LPush(queueInputKey(queue.Name), p.getString())
	queue.trackStats(queueInputRateKey(queue.Name), 1, true)
	return answer.Err()
}

// RequeueFailed moves all failed packages back to the input queue
func (queue *Queue) RequeueFailed() error {
	l := queue.GetFailedLength()
	// TODO implement this in lua
	for l > 0 {
		answer := queue.redisClient.RPopLPush(queueFailedKey(queue.Name), queueInputKey(queue.Name))
		if answer.Err() != nil {
			return answer.Err()
		}
		queue.trackStats(queueInputRateKey(queue.Name), 1, true)
		l--
	}
	return nil
}

// ResetInput deletes all packages from the input queue
func (queue *Queue) ResetInput() error {
	answer := queue.redisClient.Del(queueInputKey(queue.Name))
	return answer.Err()
}

// ResetFailed deletes all packages from the failed queue
func (queue *Queue) ResetFailed() error {
	answer := queue.redisClient.Del(queueFailedKey(queue.Name))
	return answer.Err()
}

// GetInputLength returns the number of packages in the input queue
func (queue *Queue) GetInputLength() int64 {
	l := queue.redisClient.LLen(queueInputKey(queue.Name))
	return l.Val()
}

// GetFailedLength returns the number of packages in the failed queue
func (queue *Queue) GetFailedLength() int64 {
	l := queue.redisClient.LLen(queueFailedKey(queue.Name))
	return l.Val()
}

func (queue *Queue) getConsumers() (consumers []string, err error) {
	answer := queue.redisClient.SMembers(queueWorkersKey(queue.Name))
	return answer.Val(), answer.Err()
}

func (queue *Queue) trackStats(name string, value int64, incr bool) {
	dp := &dataPoint{name: name, value: value, incr: incr, unix: time.Now().UTC().Unix()}
	queue.statsChan <- dp
}

func (queue *Queue) startStatsWriter() {
	queue.statsCache = make(map[int64]map[string]int64)
	queue.statsChan = make(chan *dataPoint, 2E6)
	queue.startStatsCache()
	go func() {
		for {
			now := time.Now().UTC().Unix()
			queue.trackStats(queueInputSizeKey(queue.Name), queue.GetInputLength(), false)
			queue.trackStats(queueFailedSizeKey(queue.Name), queue.GetFailedLength(), false)
			fmt.Println("checking to write")
			queue.writeStatsCacheToRedis(now)
			time.Sleep(1 * time.Second)
		}
	}()
	return
}

func (queue *Queue) startStatsCache() {
	go func() {
		for dp := range queue.statsChan {
			if queue.statsCache[dp.unix] == nil {
				queue.statsCache[dp.unix] = make(map[string]int64)
			}
			if dp.incr {
				queue.statsCache[dp.unix][dp.name] += dp.value
			} else {
				queue.statsCache[dp.unix][dp.name] = dp.value
			}
		}
	}()
	return
}

func (queue *Queue) writeStatsCacheToRedis(now int64) {
	fmt.Printf("start writing for %s -> %d\n", queue.Name, now)

	for sec := range queue.statsCache {
		if sec >= now-1 {
			fmt.Println("skipped now")
			continue
		}

		for name, value := range queue.statsCache[sec] {
			key := fmt.Sprintf("%s::%d", name, sec)
			// incrby can handle the situation where multiple inputs are counted
			if strings.Contains(name, "rate") {
				queue.redisClient.IncrBy(key, value)
			} else {
				queue.redisClient.Set(key, strconv.FormatInt(value, 10))
			}
			fmt.Printf("written stats: %s -> %d\n", key, value)

			// save stats to redis with 2h expiration
			queue.redisClient.Expire(key, 7200)
		}
		delete(queue.statsCache, sec)
		fmt.Printf("second written %d\n", sec)
	}
	fmt.Printf("block written for %s\n\n\n", queue.Name)
}

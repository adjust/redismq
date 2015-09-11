package redismq

import (
	"fmt"
	"strconv"
	"time"

	"gopkg.in/redis.v3"
)

// Queue is the central element of this library.
// Packages can be put into or get from the queue.
// To read from a queue you need a consumer.
type Queue struct {
	redisClient    *redis.Client
	Name           string
	rateStatsCache map[int64]map[string]int64
	rateStatsChan  chan (*dataPoint)
	lastStatsWrite int64
}

type dataPoint struct {
	name  string
	value int64
	incr  bool
}

// CreateQueue return a queue that you can Put() or AddConsumer() to
// Works like SelectQueue for existing queues
func CreateQueue(redisHost, redisPort, redisPassword string, redisDB int64, name string) *Queue {
	return newQueue(redisHost, redisPort, redisPassword, redisDB, name)
}

// SelectQueue returns a Queue if a queue with the name exists
func SelectQueue(redisHost, redisPort, redisPassword string, redisDB int64, name string) (queue *Queue, err error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisHost + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDB,
	})
	defer redisClient.Close()

	isMember, err := redisClient.SIsMember(masterQueueKey(), name).Result()
	if err != nil {
		return nil, err
	}
	if !isMember {
		return nil, fmt.Errorf("queue with this name doesn't exist")
	}

	return newQueue(redisHost, redisPort, redisPassword, redisDB, name), nil
}

func newQueue(redisHost, redisPort, redisPassword string, redisDB int64, name string) *Queue {
	q := &Queue{Name: name}
	q.redisClient = redis.NewClient(&redis.Options{
		Addr:     redisHost + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDB,
	})
	q.redisClient.SAdd(masterQueueKey(), name)
	q.startStatsWriter()
	return q
}

// Delete clears all input and failed queues as well as all consumers
// will not proceed as long as consumers are running
func (queue *Queue) Delete() error {
	consumers, err := queue.getConsumers()
	if err != nil {
		return err
	}

	for _, name := range consumers {
		if queue.isActiveConsumer(name) {
			return fmt.Errorf("cannot delete queue with active consumers")
		}

		consumer, err := queue.AddConsumer(name)
		if err != nil {
			return err
		}

		err = consumer.ResetWorking()
		if err != nil {
			return err
		}

		err = queue.redisClient.SRem(queueWorkersKey(queue.Name), name).Err()
		if err != nil {
			return err
		}
	}

	err = queue.ResetInput()
	if err != nil {
		return err
	}

	err = queue.ResetFailed()
	if err != nil {
		return err
	}

	err = queue.redisClient.SRem(masterQueueKey(), queue.Name).Err()
	if err != nil {
		return err
	}
	err = queue.redisClient.Del(queueWorkersKey(queue.Name)).Err()
	if err != nil {
		return err
	}

	queue.redisClient.Close()

	return nil
}

// Put writes the payload into the input queue
func (queue *Queue) Put(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: queue}
	lpush := queue.redisClient.LPush(queueInputKey(queue.Name), p.getString())
	queue.incrRate(queueInputRateKey(queue.Name), 1)
	return lpush.Err()
}

// RequeueFailed moves all failed packages back to the input queue
func (queue *Queue) RequeueFailed() error {
	l := queue.GetFailedLength()
	// TODO implement this in lua
	for l > 0 {
		err := queue.redisClient.RPopLPush(queueFailedKey(queue.Name), queueInputKey(queue.Name)).Err()
		if err != nil {
			return err
		}
		queue.incrRate(queueInputRateKey(queue.Name), 1)
		l--
	}
	return nil
}

// ResetInput deletes all packages from the input queue
func (queue *Queue) ResetInput() error {
	return queue.redisClient.Del(queueInputKey(queue.Name)).Err()
}

// ResetFailed deletes all packages from the failed queue
func (queue *Queue) ResetFailed() error {
	return queue.redisClient.Del(queueFailedKey(queue.Name)).Err()
}

// GetInputLength returns the number of packages in the input queue
func (queue *Queue) GetInputLength() int64 {
	return queue.redisClient.LLen(queueInputKey(queue.Name)).Val()
}

// GetFailedLength returns the number of packages in the failed queue
func (queue *Queue) GetFailedLength() int64 {
	return queue.redisClient.LLen(queueFailedKey(queue.Name)).Val()
}

func (queue *Queue) getConsumers() (consumers []string, err error) {
	return queue.redisClient.SMembers(queueWorkersKey(queue.Name)).Result()
}

func (queue *Queue) incrRate(name string, value int64) {
	dp := &dataPoint{name: name, value: value}
	queue.rateStatsChan <- dp
}

func (queue *Queue) startStatsWriter() {
	queue.rateStatsCache = make(map[int64]map[string]int64)
	queue.rateStatsChan = make(chan *dataPoint, 2E6)
	writing := false
	go func() {
		for dp := range queue.rateStatsChan {
			now := time.Now().UTC().Unix()
			if queue.rateStatsCache[now] == nil {
				queue.rateStatsCache[now] = make(map[string]int64)
			}
			queue.rateStatsCache[now][dp.name] += dp.value
			if now > queue.lastStatsWrite && !writing {
				writing = true
				queue.writeStatsCacheToRedis(now)
				writing = false
			}
		}
	}()
	return
}

func (queue *Queue) writeStatsCacheToRedis(now int64) {
	for sec := range queue.rateStatsCache {
		if sec >= now-1 {
			continue
		}

		for name, value := range queue.rateStatsCache[sec] {
			key := fmt.Sprintf("%s::%d", name, sec)
			// incrby can handle the situation where multiple inputs are counted
			queue.redisClient.IncrBy(key, value)
			// save stats to redis with 2h expiration
			queue.redisClient.Expire(key, 2*time.Hour)
		}
		// track queue lengths
		inputKey := fmt.Sprintf("%s::%d", queueInputSizeKey(queue.Name), now)
		failKey := fmt.Sprintf("%s::%d", queueFailedSizeKey(queue.Name), now)
		queue.redisClient.Set(inputKey, strconv.FormatInt(queue.GetInputLength(), 10), 2*time.Hour)
		queue.redisClient.Set(failKey, strconv.FormatInt(queue.GetFailedLength(), 10), 2*time.Hour)

		delete(queue.rateStatsCache, sec)
	}
	queue.lastStatsWrite = now
}

// AddConsumer returns a conumser that can write from the queue
func (queue *Queue) AddConsumer(name string) (c *Consumer, err error) {
	c = &Consumer{Name: name, Queue: queue}
	//check uniqueness and start heartbeat
	added, err := queue.redisClient.SAdd(queueWorkersKey(queue.Name), name).Result()
	if err != nil {
		return nil, err
	}
	if added == 0 {
		if queue.isActiveConsumer(name) {
			return nil, fmt.Errorf("consumer with this name is already active")
		}
	}
	c.startHeartbeat()
	return c, nil
}

func (queue *Queue) isActiveConsumer(name string) bool {
	val := queue.redisClient.Get(consumerHeartbeatKey(queue.Name, name)).Val()
	return val == "ping"
}

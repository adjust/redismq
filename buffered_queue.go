package redismq

import (
	"fmt"
	"github.com/adeven/redis"
)

type BufferedQueue struct {
	*Queue
	BufferSize  int
	Buffer      chan *Package
	nextWrite   int64
	flushStatus chan (chan bool)
}

func NewBufferedQueue(redisUrl, redisPassword string, redisDB int64, name string, bufferSize int) (q *BufferedQueue, err error) {
	q = &BufferedQueue{Queue: &Queue{Name: name}, BufferSize: bufferSize, Buffer: make(chan *Package, bufferSize), flushStatus: make(chan chan bool, 1)}
	q.redisClient = redis.NewTCPClient(redisUrl, redisPassword, redisDB)
	q.redisClient.SAdd(MasterQueueKey(), name)
	val := q.redisClient.Get(q.HeartbeatName()).Val()
	if val == "ping" {
		return nil, fmt.Errorf("buffered queue with this name is already active!")
	}

	q.startHeartbeat()
	q.startWritingBufferToRedis()
	return q, nil
}

func (self *BufferedQueue) HeartbeatName() string {
	return self.InputName() + "::buffered::heartbeat"
}

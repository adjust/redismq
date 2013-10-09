package redismq

import (
	"fmt"
	"github.com/adeven/goenv"
	"github.com/adeven/redis"
)

type BufferedQueue struct {
	*Queue
	BufferSize int
	Buffer     chan *Package
}

func NewBufferedQueue(goenv *goenv.Goenv, name string, bufferSize int) (q *BufferedQueue, err error) {
	q = &BufferedQueue{Queue: &Queue{Name: name}, BufferSize: bufferSize, Buffer: make(chan *Package, bufferSize)}
	host, port, db := goenv.GetRedis()
	q.redisClient = redis.NewTCPClient(host+":"+port, "", int64(db))
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

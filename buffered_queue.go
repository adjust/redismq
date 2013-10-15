package redismq

import (
	"fmt"
	"github.com/adeven/redis"
	"time"
)

// BufferedQueue provides an queue with buffered writes for increased performance.
// Only one buffered queue (per name) can be started at a time.
// Before terminating the queue should be flushed using FlushBuffer() to avoid package loss
type BufferedQueue struct {
	*Queue
	BufferSize  int
	Buffer      chan *Package
	nextWrite   int64
	flushStatus chan (chan bool)
}

// NewBufferedQueue returns BufferedQueue.
// To start writing the buffer to redis use Start().
// Optimal BufferSize seems to be around 200.
func NewBufferedQueue(redisURL, redisPassword string, redisDB int64, name string, bufferSize int) (q *BufferedQueue) {
	q = &BufferedQueue{
		Queue:       &Queue{Name: name},
		BufferSize:  bufferSize,
		Buffer:      make(chan *Package, bufferSize),
		flushStatus: make(chan chan bool, 1),
	}
	q.redisClient = redis.NewTCPClient(redisURL, redisPassword, redisDB)
	return q
}

func (queue *BufferedQueue) heartbeatName() string {
	return queue.inputName() + "::buffered::heartbeat"
}

// Start dispatches the background writer that flushes the buffer.
// If there is already a BufferedQueue running it will return an error.
func (queue *BufferedQueue) Start() error {
	queue.redisClient.SAdd(masterQueueKey(), queue.Name)
	val := queue.redisClient.Get(queue.heartbeatName()).Val()
	if val == "ping" {
		return fmt.Errorf("buffered queue with this name is already started")
	}
	queue.startHeartbeat()
	queue.startWritingBufferToRedis()
	return nil
}

// Put writes the payload to the buffer
func (queue *BufferedQueue) Put(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: queue}
	queue.Buffer <- p
	return nil
}

// FlushBuffer tells the background writer to flush the buffer to redis
func (queue *BufferedQueue) FlushBuffer() {
	flushing := make(chan bool, 1)
	queue.flushStatus <- flushing
	<-flushing
	return
}

func (queue *BufferedQueue) startWritingBufferToRedis() {
	go func() {
		queue.nextWrite = time.Now().Unix()
		for {
			if len(queue.Buffer) == cap(queue.Buffer) || time.Now().Unix() >= queue.nextWrite {
				size := len(queue.Buffer)
				queue.redisClient.Pipelined(func(c *redis.PipelineClient) {
					a := []string{}
					for i := 0; i < size; i++ {
						p := <-queue.Buffer
						a = append(a, p.getString())
					}
					c.LPush(queue.inputName(), a...)
					c.IncrBy(queue.inputCounterName(), int64(size))
				})
				for i := 0; i < len(queue.flushStatus); i++ {
					c := <-queue.flushStatus
					c <- true
				}
				queue.nextWrite = time.Now().Unix() + 1
			}
			if len(queue.Buffer) == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
}

func (queue *BufferedQueue) startHeartbeat() {
	go func() {
		for {
			queue.redisClient.SetEx(queue.heartbeatName(), 1, "ping")
			time.Sleep(500 * time.Millisecond)
		}
	}()
}

package redismq

import (
	"github.com/adeven/redis"
	"time"
)

func (self *BufferedQueue) Put(payload string) error {
	p := &Package{CreatedAt: time.Now(), Payload: payload, Queue: self}
	self.Buffer <- p
	return nil
}

func (self *BufferedQueue) startWritingBufferToRedis() {
	go func() {
		nextWrite := time.Now().Unix()
		for {
			if len(self.Buffer) == cap(self.Buffer) || time.Now().Unix() >= nextWrite {

				size := len(self.Buffer)
				self.redisClient.Pipelined(func(c *redis.PipelineClient) {
					for i := 0; i < size; i++ {
						p := <-self.Buffer
						c.LPush(self.InputName(), p.GetString())
					}
					c.IncrBy(self.InputCounterName(), int64(size))
				})
				nextWrite = time.Now().Unix() + 1
			}
		}
	}()
}

func (self *BufferedQueue) startHeartbeat() {
	go func() {
		for {
			self.redisClient.SetEx(self.HeartbeatName(), 1, "ping")
			time.Sleep(500 * time.Millisecond)
		}
	}()
}

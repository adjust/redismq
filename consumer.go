package redismq

import (
	"fmt"
)

//Consumers are Watchers that have writing commands
type Consumer struct {
	*Broker
}

func (self *Queue) AddConsumer(name string) (c *Consumer, err error) {
	c = &Consumer{&Broker{Name: name, Queue: self}}
	//check uniqueness and start heartbeat
	added := self.redisClient.SAdd(self.WorkerKey(), name).Val()
	if added == 0 {
		val := self.redisClient.Get(c.HeartbeatName()).Val()
		if val == "ping" {
			return nil, fmt.Errorf("consumer with this name is already active!")
		}
	}
	c.startHeartbeat()
	return c, nil
}

func (self *Consumer) HeartbeatName() string {
	return self.WorkingName() + "::heartbeat"
}

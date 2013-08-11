package redismq

import (
	"strconv"
)

func (self *Broker) GetUnackedLength() int64 {
	l := self.Queue.redisClient.LLen(self.WorkingName())
	return l.Val()
}

func (self *Broker) GetAckRate() int64 {
	val, err := strconv.ParseInt(self.Queue.redisClient.GetSet(self.AckCounterName(), "0").Val(), 10, 64)
	if err != nil {
		return 0
	}
	return val
}

func (self *Broker) GetWorkRate() int64 {
	val, err := strconv.ParseInt(self.Queue.redisClient.GetSet(self.WorkingCounterName(), "0").Val(), 10, 64)
	if err != nil {
		return 0
	}
	return val
}

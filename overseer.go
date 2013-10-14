package redismq

import (
	"encoding/json"
	"github.com/adeven/redis"
	"log"
	"time"
)

type Overseer struct {
	redisClient   *redis.Client
	RedisUrl      string
	RedisPassword string
	RedisDb       int64
	Stats         map[string]*QueueStat
}

type QueueStat struct {
	InputRate     int64
	WorkRate      int64
	AckRate       int64
	FailRate      int64
	InputSize     int64
	UnAckSize     int64
	FailSize      int64
	ConsumerStats map[string]*ConsumerStat
}

type ConsumerStat struct {
	WorkRate  int64
	AckRate   int64
	UnAckSize int64
}

//TODO Test this?
func NewOverseer(redisUrl, redisPassword string, redisDb int64) *Overseer {
	q := &Overseer{
		RedisUrl:      redisUrl,
		RedisPassword: redisPassword,
		RedisDb:       redisDb,
		Stats:         make(map[string]*QueueStat),
	}
	q.redisClient = redis.NewTCPClient(redisUrl, redisPassword, redisDb)
	go q.WatchAllQueues()
	return q
}

func (self *Overseer) WatchAllQueues() {
	for {
		for _, queue := range self.GetAllQueues() {
			if self.Stats[queue.Name] == nil {
				self.WatchQueue(queue)
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func (self *Overseer) GetAllQueues() (queues []*Queue) {
	answer := self.redisClient.SMembers(MasterQueueKey())
	if answer.Err() != nil {
		return
	}
	for _, name := range answer.Val() {
		queues = append(queues, NewQueue(self.RedisUrl, self.RedisPassword, self.RedisDb, name))
	}
	return
}

func (self *Overseer) WatchQueue(queue *Queue) {
	self.Stats[queue.Name] = &QueueStat{ConsumerStats: make(map[string]*ConsumerStat)}
	go self.Poll(queue)
}

func (self *Overseer) Poll(queue *Queue) {
	for {
		self.Stats[queue.Name].InputRate = queue.GetInputRate()
		self.Stats[queue.Name].FailRate = queue.GetFailedRate()
		self.Stats[queue.Name].InputSize = queue.GetInputLength()
		self.Stats[queue.Name].FailSize = queue.GetFailedLength()

		self.Stats[queue.Name].WorkRate = 0
		self.Stats[queue.Name].AckRate = 0
		self.Stats[queue.Name].UnAckSize = 0

		brokers, err := queue.GetBrokers()
		if err != nil {
			continue //TODO handle this
		}

		for _, broker := range brokers {
			self.Stats[queue.Name].ConsumerStats[broker.Name] = &ConsumerStat{}
			self.Stats[queue.Name].ConsumerStats[broker.Name].AckRate = broker.GetAckRate()
			self.Stats[queue.Name].ConsumerStats[broker.Name].WorkRate = broker.GetWorkRate()
			self.Stats[queue.Name].ConsumerStats[broker.Name].UnAckSize = broker.GetUnackedLength()

			self.Stats[queue.Name].AckRate += self.Stats[queue.Name].ConsumerStats[broker.Name].AckRate
			self.Stats[queue.Name].WorkRate += self.Stats[queue.Name].ConsumerStats[broker.Name].WorkRate
			self.Stats[queue.Name].UnAckSize += self.Stats[queue.Name].ConsumerStats[broker.Name].UnAckSize
		}

		time.Sleep(1 * time.Second)
	}

}

func (self *Overseer) OutputToString() string {
	json, err := json.Marshal(self)
	if err != nil {
		log.Fatalf("ERROR MARSHALLING OVERSEER %s", err.Error())
	}
	return string(json)
}

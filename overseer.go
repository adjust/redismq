package redismq

import (
	"encoding/json"
	"github.com/adeven/goenv"
	"github.com/adeven/redis"
	"log"
	"time"
)

type Overseer struct {
	redisClient *redis.Client
	goenv       *goenv.Goenv
	Stats       map[string]*QueueStat
}

type QueueStat struct {
	InputRate     int64
	WorkRate      int64
	AckRate       int64
	FailRate      int64
	InputSize     int64
	WorkSize      int64
	FailSize      int64
	ConsumerStats map[string]*ConsumerStat
}

type ConsumerStat struct {
	WorkRate int64
	AckRate  int64
}

//TODO Test this?
func NewOverseer(goenv *goenv.Goenv) *Overseer {
	q := &Overseer{goenv: goenv, Stats: make(map[string]*QueueStat)}
	host, port, db := goenv.GetRedis()
	q.redisClient = redis.NewTCPClient(host+":"+port, "", int64(db))

	for _, queue := range q.GetAllQueues() {
		q.WatchQueue(queue)
	}
	return q
}

func (self *Overseer) GetAllQueues() (queues []*Queue) {
	answer := self.redisClient.SMembers(MasterQueueKey())
	if answer.Err() != nil {
		return
	}
	for _, name := range answer.Val() {
		queues = append(queues, NewQueue(self.goenv, name))
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

		brokers, err := queue.GetBrokers()
		if err != nil {
			continue //TODO handle this
		}

		for _, broker := range brokers {
			self.Stats[queue.Name].ConsumerStats[broker.Name] = &ConsumerStat{}
			self.Stats[queue.Name].ConsumerStats[broker.Name].AckRate = broker.GetAckRate()
			self.Stats[queue.Name].ConsumerStats[broker.Name].WorkRate = broker.GetWorkRate()

			self.Stats[queue.Name].AckRate += self.Stats[queue.Name].ConsumerStats[broker.Name].AckRate
			self.Stats[queue.Name].WorkRate += self.Stats[queue.Name].ConsumerStats[broker.Name].WorkRate
		}
		json, _ := json.Marshal(self)
		log.Println(string(json))

		time.Sleep(1 * time.Second)
	}

}

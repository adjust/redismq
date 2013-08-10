package redismq

import (
	"encoding/json"
	"github.com/adeven/goenv"
	"github.com/adeven/redis"
	"log"
	"time"
)

type QueueWatcher struct {
	redisClient *redis.Client
	goenv       *goenv.Goenv
	Stats       map[string]*QueueStat
}

type QueueStat struct {
	InputRate  int64
	WorkRate   int64
	AckRate    int64
	FailRate   int64
	InputSize  int64
	WorkSize   int64
	FailSize   int64
	WokerStats []*WorkerStat
}

type WorkerStat struct {
	Name     string
	WorkRate int64
	AckRate  int64
}

//TODO Test this?
func NewQueueWatcher(goenv *goenv.Goenv) *QueueWatcher {
	q := &QueueWatcher{goenv: goenv, Stats: make(map[string]*QueueStat)}
	host, port, db := goenv.GetRedis()
	q.redisClient = redis.NewTCPClient(host+":"+port, "", int64(db))

	for _, queue := range q.GetAllQueues() {
		q.WatchQueue(queue)
	}
	return q
}

func (self *QueueWatcher) GetAllQueues() (queues []*Queue) {
	answer := self.redisClient.SMembers(MasterQueueKey())
	if answer.Err() != nil {
		return
	}
	for _, name := range answer.Val() {
		queues = append(queues, NewQueue(self.goenv, name))
	}
	return
}

func (self *QueueWatcher) WatchQueue(queue *Queue) {
	self.Stats[queue.Name] = &QueueStat{}
	go self.Poll(queue)
}

func (self *QueueWatcher) Poll(queue *Queue) {
	for {
		self.Stats[queue.Name].InputRate = queue.GetInputRate()
		self.Stats[queue.Name].FailRate = queue.GetFailedRate()
		self.Stats[queue.Name].InputSize = queue.GetInputLength()
		self.Stats[queue.Name].FailSize = queue.GetFailedLength()

		json, _ := json.Marshal(self)
		log.Println(string(json))

		time.Sleep(1 * time.Second)
	}

}

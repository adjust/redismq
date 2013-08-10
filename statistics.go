package redismq

import (
	"encoding/json"
	"github.com/adeven/goenv"
	"github.com/adeven/redis"
	"log"
	"strconv"
	"time"
)

type QueueWatcher struct {
	redisClient *redis.Client
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

func NewQueueWatcher(goenv *goenv.Goenv) *QueueWatcher {
	q := &QueueWatcher{Stats: make(map[string]*QueueStat)}
	host, port, db := goenv.GetRedis()
	q.redisClient = redis.NewTCPClient(host+":"+port, "", int64(db))
	for _, queue := range q.GetAllQueues() {
		q.WatchQueue(queue)
	}
	return q
}

func (self *QueueWatcher) GetAllQueues() (queues []string) {
	answer := self.redisClient.SMembers(MasterQueueKey())
	if answer.Err() != nil {
		return
	}
	return answer.Val()
}

//TODO write tests
//TODO find a nicer way not to double these functions but also not to open a new redisClient for each queue
func (self *QueueWatcher) InputName(queueName string) string {
	return "redismq::" + queueName
}

func (self *QueueWatcher) WorkingName(queueName, consumer string) string {
	return "redismq::" + queueName + "::working::" + consumer
}

func (self *QueueWatcher) FailedName(queueName string) string {
	return "redismq::" + queueName + "::failed"
}

func (self *QueueWatcher) InputCounterName(queueName string) string {
	return self.InputName(queueName) + "::counter"
}

func (self *QueueWatcher) WorkingCounterName(queueName, consumer string) string {
	return self.WorkingName(queueName, consumer) + "::counter"
}

func (self *QueueWatcher) FailedCounterName(queueName string) string {
	return self.FailedName(queueName) + "::counter"
}

// func (queueWatcher *QueueWatcher) GetAllQueueWorkers(queueName string) (workers []string) {
// 	answer := queueWatcher.redisClient.SMembers(MasterQueueKey())
// 	if answer.Err != nil {
// 		return
// 	}
// 	return answer.Val()
// }

func (self *QueueWatcher) WatchQueue(queueName string) {
	log.Println(queueName)
	self.Stats[queueName] = &QueueStat{}
	go self.Poll(queueName)
}

func (self *QueueWatcher) Poll(queueName string) {
	for {
		self.Stats[queueName].InputRate = self.redisClient.GetSet(self.InputCounterName(queueName), "0").Val()
		self.Stats[queueName].FailRate = self.redisClient.GetSet(self.FailedCounterName(queueName), "0").Val()

		json, _ := json.Marshal(self)
		log.Println(string(json))

		time.Sleep(1 * time.Second)
	}

}

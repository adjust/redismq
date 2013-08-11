package redismq

//Watcher are an extra Model so a different instance can watch
//consumers from e.g. another server without being able to write
type Broker struct {
	Name  string
	Queue *Queue
}

func (self *Broker) GetName() string {
	return self.Name
}

func (self *Broker) GetQueue() *Queue {
	return self.Queue
}

func (self *Queue) GetBrokers() (brokers []*Broker, err error) {
	answer := self.redisClient.SMembers(self.WorkerKey())
	if answer.Err() != nil {
		return nil, err
	}
	for _, name := range answer.Val() {
		w := &Broker{Name: name, Queue: self}
		brokers = append(brokers, w)
	}
	return
}
func (self *Broker) HasUnacked() bool {
	if self.GetUnackedLength() != 0 {
		return true
	}
	return false
}

func (self *Broker) WorkingName() string {
	return self.Queue.ConsumerWorkingPrefix() + "::" + self.Name
}
func (self *Broker) WorkingCounterName() string {
	return self.WorkingName() + "::counter"
}
func (self *Broker) AckCounterName() string {
	return self.Queue.ConsumerAckPrefix() + "::" + self.Name + "::counter"
}

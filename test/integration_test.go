package main

import (
	//"fmt"
	"github.com/adeven/goenv"
	"github.com/adeven/redismq"
	. "github.com/matttproud/gocheck"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	goenv    *goenv.Goenv
	queue    *redismq.Queue
	consumer *redismq.Consumer
}

var _ = Suite(&TestSuite{})

func (suite *TestSuite) SetUpSuite(c *C) {
	runtime.GOMAXPROCS(8)
	rand.Seed(time.Now().UTC().UnixNano())
	suite.goenv = goenv.NewGoenv("../example/config.yml", "gotesting", "../example/log/test.log")
	suite.queue = redismq.NewQueue(suite.goenv, "teststuff")
	suite.consumer, _ = suite.queue.AddConsumer("testconsumer")
}

func (suite *TestSuite) SetUpTest(c *C) {
	suite.queue.ResetInput()
	suite.queue.ResetFailed()
	suite.consumer.ResetWorking()
}

//should not allow a second consumer with the same name
func (suite *TestSuite) TestUniqueConsumer(c *C) {
	_, err := suite.queue.AddConsumer("testconsumer")
	c.Check(err, Not(Equals), nil)
}

//should put package into queue
func (suite *TestSuite) TestPutGetAndAck(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")
	c.Check(p.Ack(), Equals, nil)
	c.Check(suite.consumer.HasUnacked(), Equals, false)
}

//should queue packages
func (suite *TestSuite) TestQueuingPackages(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	c.Check(suite.queue.GetInputLength(), Equals, int64(100))
}

//shouldn't get 2nd package for consumer
func (suite *TestSuite) TestSecondGet(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)

	_, err = suite.consumer.Get()
	c.Check(err, Not(Equals), nil)

	err = p.Ack()
	c.Check(err, Equals, nil)

	_, err = suite.consumer.Get()
	c.Check(err, Equals, nil)
}

//test waiting for get
func (suite *TestSuite) TestWaitForGet(c *C) {
	go func() {
		p, err := suite.consumer.Get()
		c.Assert(err, Equals, nil)
		c.Check(p.Payload, Equals, "testpayload")
	}()
	time.Sleep(1 * time.Second)
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
}

//should get package for second consumer
func (suite *TestSuite) TestSecondConsumer(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")

	consumer, err := suite.queue.AddConsumer("testconsumer2")
	c.Assert(err, Equals, nil)
	consumer.ResetWorking()

	p2, err := consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload")
}

//should reject package to requeue
func (suite *TestSuite) TestRequeue(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Reject(true), Equals, nil)

	p2, err2 := suite.consumer.Get()
	c.Assert(err2, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload")
}

//should reject package to failed
func (suite *TestSuite) TestFailed(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Reject(false), Equals, nil)

	c.Check(suite.queue.GetFailedLength(), Equals, int64(1))
}

//should get unacked package(s)
func (suite *TestSuite) TestGetUnacked(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	_, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	//Assume that consumer crashed and receives err on get

	_, err = suite.consumer.Get()
	c.Assert(err, Not(Equals), nil)

	p, err := suite.consumer.GetUnacked()
	c.Assert(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")
	c.Check(p.Ack(), Equals, nil)

	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(0))
}

//should requeue failed
func (suite *TestSuite) TestRequeueFailed(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	for i := 0; i < 100; i++ {
		p, err := suite.consumer.Get()
		c.Check(err, Equals, nil)
		c.Check(p.Reject(false), Equals, nil)
	}
	c.Check(suite.queue.GetFailedLength(), Equals, int64(100))
	c.Check(suite.queue.RequeueFailed(), Equals, nil)
	c.Check(suite.queue.GetFailedLength(), Equals, int64(0))
	c.Check(suite.queue.GetInputLength(), Equals, int64(100))
}

//should get failed
func (suite *TestSuite) TestGetFailed(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Reject(false), Equals, nil)
	c.Check(suite.queue.GetFailedLength(), Equals, int64(1))

	p2, err := suite.consumer.GetFailed()
	c.Assert(err, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload")
	c.Check(p2.Ack(), Equals, nil)
	c.Check(suite.queue.GetFailedLength(), Equals, int64(0))
	c.Check(suite.queue.GetInputLength(), Equals, int64(0))

	//try getting smth from empty failed queue
	_, err = suite.consumer.GetFailed()
	c.Check(err, Not(Equals), nil)
}

//should handle multiple queues
func (suite *TestSuite) TestSecondQueue(c *C) {
	secondQueue := redismq.NewQueue(suite.goenv, "teststuff2")
	secondConsumer, err := secondQueue.AddConsumer("testconsumer")
	c.Assert(err, Equals, nil)

	secondQueue.ResetInput()
	secondQueue.ResetFailed()
	secondConsumer.ResetWorking()
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	c.Check(secondQueue.Put("testpayload2"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")

	p2, err := secondConsumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload2")

	secondQueue.ResetInput()
	secondQueue.ResetFailed()
	secondConsumer.ResetWorking()
}

//should handle huge payloads
func (suite *TestSuite) TestHugePayload(c *C) {
	//10MB payload
	payload := randomString(1024 * 1024 * 10)

	c.Check(suite.queue.Put(payload), Equals, nil)
	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Payload, Equals, payload)
	c.Check(p.Ack(), Equals, nil)
	c.Check(suite.consumer.HasUnacked(), Equals, false)
}

//should get multiple packages from queue and ack all of them
func (suite *TestSuite) TestMultiGetAndAck(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	c.Check(suite.queue.GetInputLength(), Equals, int64(100))

	p, err := suite.consumer.MultiGet(100)
	c.Assert(err, Equals, nil)
	c.Check(suite.queue.GetInputLength(), Equals, int64(0))
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(100))

	for j := range p {
		c.Check(p[j].Payload, Equals, "testpayload")
	}
	c.Check(p[len(p)-1].MultiAck(), Equals, nil)
	c.Check(suite.consumer.HasUnacked(), Equals, false)
}

//should get multiple packages from queue and ack half of them
func (suite *TestSuite) TestMultiGetAndPartialAck(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	c.Check(suite.queue.GetInputLength(), Equals, int64(100))

	p, err := suite.consumer.MultiGet(100)
	c.Assert(err, Equals, nil)
	c.Check(suite.queue.GetInputLength(), Equals, int64(0))
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(100))

	for j := range p {
		c.Check(p[j].Payload, Equals, "testpayload")
	}
	c.Check(p[49].MultiAck(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(50))
}

//should get multiple packages from queue and not reject the middle one
func (suite *TestSuite) TestMultiGetAndBlockedReject(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	p, err := suite.consumer.MultiGet(100)
	c.Assert(err, Equals, nil)
	c.Check(p[49].Reject(false), Not(Equals), nil)
	c.Check(p[48].MultiAck(), Equals, nil)
	c.Check(p[49].Reject(false), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(50))
}

//should get multiple packages from queue and ack them in one after another
func (suite *TestSuite) TestMultiGetAndMultiAck(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	p, err := suite.consumer.MultiGet(100)
	c.Assert(err, Equals, nil)
	c.Check(p[49].MultiAck(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(50))
	c.Check(p[49].MultiAck(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(50))
	c.Check(p[50].MultiAck(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(49))
	c.Check(p[98].MultiAck(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(1))
}

//should not wait on multi get if less than requested amount of packages are in queue
func (suite *TestSuite) TestMultiGetNoWait(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	p, err := suite.consumer.MultiGet(100)
	c.Assert(err, Equals, nil)
	c.Check(p[len(p)-1].MultiAck(), Equals, nil)
	c.Check(suite.consumer.HasUnacked(), Equals, false)
}

//TODO write stats watcher
//should get numbers of consumers

//should get publish rate for input

//should get consume rate for input

//should get consume rate for consumer

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

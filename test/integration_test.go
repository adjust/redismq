package main

import (
	"github.com/adeven/goenv"
	"github.com/adeven/rqueue"
	. "github.com/matttproud/gocheck"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	queue    *rqueue.Queue
	consumer string
}

var _ = Suite(&TestSuite{})

func (suite *TestSuite) SetUpSuite(c *C) {
	goenv := goenv.NewGoenv("../example/config.yml", "gotesting", "../example/log/test.log")
	suite.queue = rqueue.NewQueue(goenv, "teststuff")
	suite.consumer = "testconsumer"
}

func (suite *TestSuite) SetUpTest(c *C) {
	suite.queue.ResetInput()
	suite.queue.ResetFailed()
	suite.queue.ResetWorking(suite.consumer)
	suite.queue.ResetWorking(suite.consumer + "2")
}

//should put package into queue
func (suite *TestSuite) TestPutGetAndAck(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	p, err := suite.queue.Get(suite.consumer)
	c.Check(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")
	c.Check(p.Ack(), Equals, nil)
	c.Check(suite.queue.HasUnacked(suite.consumer), Equals, false)
}

//should queue packages
func (suite *TestSuite) TestQueuingPackages(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	c.Check(suite.queue.InputLength(), Equals, int64(100))
}

//shouldn't get 2nd package for consumer
func (suite *TestSuite) TestSecondGet(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.queue.Get(suite.consumer)
	c.Check(err, Equals, nil)

	_, err = suite.queue.Get(suite.consumer)
	c.Check(err, Not(Equals), nil)

	err = p.Ack()
	c.Check(err, Equals, nil)

	_, err = suite.queue.Get(suite.consumer)
	c.Check(err, Equals, nil)
}

//test waiting for get
func (suite *TestSuite) TestWaitForGet(c *C) {
	go func() {
		p, err := suite.queue.Get(suite.consumer)
		c.Check(err, Equals, nil)
		c.Check(p.Payload, Equals, "testpayload")
	}()
	time.Sleep(1 * time.Second)
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
}

//should get package for second consumer
func (suite *TestSuite) TestSecondConsumer(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.queue.Get(suite.consumer)
	c.Check(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")

	p2, err2 := suite.queue.Get(suite.consumer + "2")
	c.Check(err2, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload")
}

//should reject package to requeue
func (suite *TestSuite) TestRequeue(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.queue.Get(suite.consumer)
	c.Check(err, Equals, nil)
	c.Check(p.Reject(true), Equals, nil)

	p2, err2 := suite.queue.Get(suite.consumer)
	c.Check(err2, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload")
}

//should reject package to failed
func (suite *TestSuite) TestFailed(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.queue.Get(suite.consumer)
	c.Check(err, Equals, nil)
	c.Check(p.Reject(false), Equals, nil)

	c.Check(suite.queue.FailedLength(), Equals, int64(1))
}

//should get unacked package(s)
func (suite *TestSuite) TestGetUnacked(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	_, err := suite.queue.Get(suite.consumer)
	c.Check(err, Equals, nil)
	//Assume that consumer crashed and receives err on get

	_, err = suite.queue.Get(suite.consumer)
	c.Check(err, Not(Equals), nil)

	p, err := suite.queue.GetUnacked(suite.consumer)
	c.Check(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")
	c.Check(p.Ack(), Equals, nil)

	c.Check(suite.queue.UnackedLength(suite.consumer), Equals, int64(0))
}

//should requeue failed
func (suite *TestSuite) TestRequeueFailed(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	for i := 0; i < 100; i++ {
		p, err := suite.queue.Get(suite.consumer)
		c.Check(err, Equals, nil)
		c.Check(p.Reject(false), Equals, nil)
	}
	c.Check(suite.queue.FailedLength(), Equals, int64(100))
	c.Check(suite.queue.RequeueFailed(), Equals, nil)
	c.Check(suite.queue.FailedLength(), Equals, int64(0))
	c.Check(suite.queue.InputLength(), Equals, int64(100))
}

//should get failed
func (suite *TestSuite) TestGetFailed(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.queue.Get(suite.consumer)
	c.Check(err, Equals, nil)
	c.Check(p.Reject(false), Equals, nil)
	c.Check(suite.queue.FailedLength(), Equals, int64(1))

	p2, err := suite.queue.GetFailed(suite.consumer)
	c.Check(err, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload")
	c.Check(p2.Ack(), Equals, nil)
	c.Check(suite.queue.FailedLength(), Equals, int64(0))
	c.Check(suite.queue.InputLength(), Equals, int64(0))

	//try getting smth from empty failed queue
	_, err = suite.queue.GetFailed(suite.consumer)
	c.Check(err, Not(Equals), nil)
}

//should handle multiple queues

//should handle huge payloads

//should get length of input queue

//should get length of failed queue

//should get numbers of consumers

//should get publish rate for input

//should get consume rate for input

//should get consume rate for consumer

//benchmark single publisher 1k payload

//benchmark single publisher 4k payload

//benchmark four publishers 1k payload

//benchmark four publisher 4k payload

//benchmark single publisher and single consumer 1k payload

//benchmark single publisher and single consumer 4k payload

//benchmark four publisher and four consumers 1k payload

//benchmark four publisher and four consumers 4k payload

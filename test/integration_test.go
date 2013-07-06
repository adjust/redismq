package main

import (
	//"fmt"
	"github.com/adeven/goenv"
	"github.com/adeven/rqueue"
	. "github.com/matttproud/gocheck"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	goenv    *goenv.Goenv
	queue    *rqueue.Queue
	consumer string
}

var _ = Suite(&TestSuite{})

func (suite *TestSuite) SetUpSuite(c *C) {
	runtime.GOMAXPROCS(4)
	rand.Seed(time.Now().UTC().UnixNano())
	suite.goenv = goenv.NewGoenv("../example/config.yml", "gotesting", "../example/log/test.log")
	suite.queue = rqueue.NewQueue(suite.goenv, "teststuff")
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
func (suite *TestSuite) TestSecondQueue(c *C) {
	secondQueue := rqueue.NewQueue(suite.goenv, "teststuff2")
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	c.Check(secondQueue.Put("testpayload2"), Equals, nil)

	p, err := suite.queue.Get(suite.consumer)
	c.Check(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")

	p2, err := secondQueue.Get(suite.consumer)
	c.Check(err, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload2")

	secondQueue.ResetInput()
	secondQueue.ResetFailed()
	secondQueue.ResetWorking(suite.consumer)
}

//should handle huge payloads
func (suite *TestSuite) TestHugePayload(c *C) {
	//10MB payload
	payload := randomString(1024 * 1024 * 10)

	c.Check(suite.queue.Put(payload), Equals, nil)
	p, err := suite.queue.Get(suite.consumer)
	c.Check(err, Equals, nil)
	c.Check(p.Payload, Equals, payload)
	c.Check(p.Ack(), Equals, nil)
	c.Check(suite.queue.HasUnacked(suite.consumer), Equals, false)
}

//TODO write stats watcher
//should get numbers of consumers

//should get publish rate for input

//should get consume rate for input

//should get consume rate for consumer

//benchmark single publisher 1k payload
func (suite *TestSuite) BenchmarkSinglePub1k(c *C) {
	payload := randomString(1024)
	for i := 0; i < c.N; i++ {
		suite.queue.Put(payload)
	}
}

//benchmark single consumer 1k payload
func (suite *TestSuite) BenchmarkSingleCon1k(c *C) {
	payload := randomString(1024)
	for i := 0; i < 100000; i++ {
		suite.queue.Put(payload)
	}
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		p, _ := suite.queue.Get(suite.consumer)
		p.Ack()
	}
}

//benchmark single publisher 4k payload
func (suite *TestSuite) BenchmarkSinglePub4k(c *C) {
	payload := randomString(1024 * 4)
	for i := 0; i < c.N; i++ {
		suite.queue.Put(payload)
	}
}

//benchmark single consumer 4k payload
func (suite *TestSuite) BenchmarkSingleCon4k(c *C) {
	payload := randomString(1024 * 4)
	for i := 0; i < 100000; i++ {
		suite.queue.Put(payload)
	}
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		p, _ := suite.queue.Get(suite.consumer)
		p.Ack()
	}
}

//benchmark four publishers 1k payload
func (suite *TestSuite) BenchmarkFourPub1k(c *C) {
	//1KB payload
	var wg sync.WaitGroup
	payload := randomString(1024)
	for i := 0; i < c.N; i++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				suite.queue.Put(payload)
				defer wg.Done()
			}()
		}
		wg.Wait()
	}
}

//benchmark four consumers 1k payload
func (suite *TestSuite) BenchmarkFourCon1k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024)
	for i := 0; i < 100000; i++ {
		suite.queue.Put(payload)
	}
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(j int) {
				p, _ := suite.queue.Get(suite.consumer + strconv.Itoa(i) + strconv.Itoa(j))
				p.Ack()
				defer wg.Done()
			}(j)
		}
		wg.Wait()
	}
}

//benchmark four publisher 4k payload
func (suite *TestSuite) BenchmarkFourPub4k(c *C) {
	//1KB payload
	var wg sync.WaitGroup
	payload := randomString(1024 * 4)
	for i := 0; i < c.N; i++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				suite.queue.Put(payload)
				defer wg.Done()
			}()
		}
		wg.Wait()
	}
}

//benchmark four consumers 4k payload
func (suite *TestSuite) BenchmarkFourCon4k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024 * 4)
	for i := 0; i < 100000; i++ {
		suite.queue.Put(payload)
	}
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(j int) {
				p, _ := suite.queue.Get(suite.consumer + strconv.Itoa(i) + strconv.Itoa(j))
				p.Ack()
				defer wg.Done()
			}(j)
		}
		wg.Wait()
	}
}

//benchmark single publisher and single consumer 1k payload
func (suite *TestSuite) BenchmarkSingPubSingCon1k(c *C) {
	//1KB payload
	var wg sync.WaitGroup
	payload := randomString(1024)
	//add first package so consumer doesn't wait for publisher
	suite.queue.Put(payload)
	for i := 0; i < c.N; i++ {
		wg.Add(1)
		go func() {
			suite.queue.Put(payload)
			defer wg.Done()
		}()
		wg.Add(1)
		go func() {
			p, _ := suite.queue.Get(suite.consumer)
			p.Ack()
			defer wg.Done()
		}()
		wg.Wait()
	}
}

//benchmark single publisher and single consumer 4k payload

//benchmark four publisher and four consumers 1k payload

//benchmark four publisher and four consumers 4k payload

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

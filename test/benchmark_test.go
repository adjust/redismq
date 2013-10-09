package main

import (
	//"fmt"
	"github.com/adeven/goenv"
	. "github.com/matttproud/gocheck"
	"github.com/adeven/redismq"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type BenchmarkSuite struct {
	goenv           *goenv.Goenv
	queue1k         *redismq.Queue
	queue4k         *redismq.Queue
	consumer1k      *redismq.Consumer
	consumer4k      *redismq.Consumer
	mutliConsumer1k []*redismq.Consumer
	mutliConsumer4k []*redismq.Consumer
}

var _ = Suite(&BenchmarkSuite{})

func (suite *BenchmarkSuite) SetUpSuite(c *C) {
	runtime.GOMAXPROCS(8)
	rand.Seed(time.Now().UTC().UnixNano())
	suite.goenv = goenv.NewGoenv("../example/config.yml", "gotesting", "../example/log/test.log")
	suite.queue1k = redismq.NewQueue(suite.goenv, "teststuff1k")
	suite.queue4k = redismq.NewQueue(suite.goenv, "teststuff4k")
	suite.consumer1k, _ = suite.queue1k.AddConsumer("testconsumer")
	suite.consumer4k, _ = suite.queue4k.AddConsumer("testconsumer")

	suite.mutliConsumer1k = make([]*redismq.Consumer, 0)
	for i := 0; i < 4; i++ {
		q := redismq.NewQueue(suite.goenv, "teststuff1k")
		c, err := q.AddConsumer("c" + strconv.Itoa(i))
		if err != nil {
			panic(err)
		}
		suite.mutliConsumer1k = append(suite.mutliConsumer1k, c)
	}
	suite.mutliConsumer4k = make([]*redismq.Consumer, 0)
	for i := 0; i < 4; i++ {
		q := redismq.NewQueue(suite.goenv, "teststuff4k")
		c, err := q.AddConsumer("c" + strconv.Itoa(i))
		if err != nil {
			panic(err)
		}
		suite.mutliConsumer4k = append(suite.mutliConsumer4k, c)
	}
}

func (suite *BenchmarkSuite) SetUpTest(c *C) {
	suite.queue1k.ResetInput()
	suite.queue1k.ResetFailed()
	suite.consumer1k.ResetWorking()
	suite.queue4k.ResetInput()
	suite.queue4k.ResetFailed()
	suite.consumer4k.ResetWorking()
	payload := randomString(1024)
	for i := 0; i < 100000; i++ {
		suite.queue1k.Put(payload)
	}
	payload = randomString(1024 * 4)
	for i := 0; i < 100000; i++ {
		suite.queue4k.Put(payload)
	}
}

//benchmark single publisher 1k payload
func (suite *BenchmarkSuite) BenchmarkSinglePub1k(c *C) {
	payload := randomString(1024)
	for i := 0; i < c.N; i++ {
		suite.queue1k.Put(payload)
	}
}

//benchmark single consumer 1k payload
func (suite *BenchmarkSuite) BenchmarkSingleCon1k(c *C) {
	for i := 0; i < c.N; i++ {
		p, _ := suite.consumer1k.Get()
		p.Ack()
	}
}

//benchmark single consumer multi 100 1k payload
func (suite *BenchmarkSuite) BenchmarkSingleConMulti1k(c *C) {
	for i := 0; i < c.N; i++ {
		p, _ := suite.consumer1k.MultiGet(100)
		p[99].MultiAck()
	}
}

//benchmark single publisher 4k payload
func (suite *BenchmarkSuite) BenchmarkSinglePub4k(c *C) {
	payload := randomString(1024 * 4)
	for i := 0; i < c.N; i++ {
		suite.queue4k.Put(payload)
	}
}

//benchmark single consumer 4k payload
func (suite *BenchmarkSuite) BenchmarkSingleCon4k(c *C) {
	for i := 0; i < c.N; i++ {
		p, _ := suite.consumer4k.Get()
		p.Ack()
	}
}

//benchmark four publishers 1k payload
func (suite *BenchmarkSuite) BenchmarkFourPub1k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024)
	for i := 0; i < c.N; i++ {
		for _, c := range suite.mutliConsumer1k {
			go func(consumer *redismq.Consumer) {
				consumer.Queue.Put(payload)
				defer wg.Done()
			}(c)
			wg.Add(1)
		}
		wg.Wait()
	}
}

//benchmark four consumers 1k payload
func (suite *BenchmarkSuite) BenchmarkFourCon1k(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < c.N; i++ {
		for _, c := range suite.mutliConsumer1k {
			go func(consumer *redismq.Consumer) {
				p, _ := consumer.Get()
				p.Ack()
				defer wg.Done()
			}(c)
			wg.Add(1)
		}
		wg.Wait()
	}
}

//benchmark four publisher 4k payload
func (suite *BenchmarkSuite) BenchmarkFourPub4k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024)
	for i := 0; i < c.N; i++ {
		for _, c := range suite.mutliConsumer4k {
			go func(consumer *redismq.Consumer) {
				consumer.Queue.Put(payload)
				defer wg.Done()
			}(c)
			wg.Add(1)
		}
		wg.Wait()
	}
}

//benchmark four consumers 4k payload
func (suite *BenchmarkSuite) BenchmarkFourCon4k(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < c.N; i++ {
		for _, c := range suite.mutliConsumer4k {
			go func(consumer *redismq.Consumer) {
				p, _ := consumer.Get()
				p.Ack()
				defer wg.Done()
			}(c)
			wg.Add(1)
		}
		wg.Wait()
	}
}

//benchmark single publisher and single consumer 1k payload
func (suite *BenchmarkSuite) BenchmarkSingPubSingCon1k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024)
	//add first package so consumer doesn't wait for publisher
	suite.queue1k.Put(payload)
	for i := 0; i < c.N; i++ {
		wg.Add(1)
		go func() {
			suite.queue1k.Put(payload)
			defer wg.Done()
		}()
		wg.Add(1)
		go func() {
			p, _ := suite.consumer1k.Get()
			p.Ack()
			defer wg.Done()
		}()
		wg.Wait()
	}
}

//benchmark single publisher and single consumer 4k payload
func (suite *BenchmarkSuite) BenchmarkSingPubSingCon4k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024 * 4)
	//add first package so consumer doesn't wait for publisher
	suite.queue1k.Put(payload)
	for i := 0; i < c.N; i++ {
		wg.Add(1)
		go func() {
			suite.queue4k.Put(payload)
			defer wg.Done()
		}()
		wg.Add(1)
		go func() {
			p, _ := suite.consumer4k.Get()
			p.Ack()
			defer wg.Done()
		}()
		wg.Wait()
	}
}

//benchmark four publisher and four consumers 1k payload
func (suite *BenchmarkSuite) BenchmarkFourPubFourCon1k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024)

	for i := 0; i < c.N; i++ {
		for _, c := range suite.mutliConsumer1k {
			go func(consumer *redismq.Consumer) {
				consumer.Queue.Put(payload)
				defer wg.Done()
			}(c)
			wg.Add(1)
		}
		for _, c := range suite.mutliConsumer1k {
			go func(consumer *redismq.Consumer) {
				p, _ := consumer.Get()
				p.Ack()
				defer wg.Done()
			}(c)
			wg.Add(1)
		}

		wg.Wait()
	}
}

//benchmark four publisher and four consumers 4k payload
func (suite *BenchmarkSuite) BenchmarkFourPubFourCon4k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024 * 4)

	for i := 0; i < c.N; i++ {
		for _, c := range suite.mutliConsumer4k {
			go func(consumer *redismq.Consumer) {
				consumer.Queue.Put(payload)
				defer wg.Done()
			}(c)
			wg.Add(1)
		}
		for _, c := range suite.mutliConsumer4k {
			go func(consumer *redismq.Consumer) {
				p, _ := consumer.Get()
				p.Ack()
				defer wg.Done()
			}(c)
			wg.Add(1)
		}

		wg.Wait()
	}
}

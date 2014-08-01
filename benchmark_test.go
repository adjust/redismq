package redismq

import (
	//"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"time"

	. "github.com/matttproud/gocheck"
)

type BenchmarkSuite struct {
	queue1k         *Queue
	queue4k         *Queue
	consumer1k      *Consumer
	consumer4k      *Consumer
	mutliConsumer1k []*Consumer
	mutliConsumer4k []*Consumer
}

var _ = Suite(&BenchmarkSuite{})

func (suite *BenchmarkSuite) SetUpSuite(c *C) {
	runtime.GOMAXPROCS(8)
	rand.Seed(time.Now().UTC().UnixNano())
	suite.queue1k = CreateQueue(redisHost, redisPort, redisPassword, redisDB, "teststuff1k")
	suite.queue4k = CreateQueue(redisHost, redisPort, redisPassword, redisDB, "teststuff4k")
	suite.consumer1k, _ = suite.queue1k.AddConsumer("testconsumer")
	suite.consumer4k, _ = suite.queue4k.AddConsumer("testconsumer")

	suite.mutliConsumer1k = make([]*Consumer, 0)
	for i := 0; i < 4; i++ {
		q := CreateQueue(redisHost, redisPort, redisPassword, redisDB, "teststuff1k")
		c, err := q.AddConsumer("c" + strconv.Itoa(i))
		if err != nil {
			panic(err)
		}
		suite.mutliConsumer1k = append(suite.mutliConsumer1k, c)
	}
	suite.mutliConsumer4k = make([]*Consumer, 0)
	for i := 0; i < 4; i++ {
		q := CreateQueue(redisHost, redisPort, redisPassword, redisDB, "teststuff4k")
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
			wg.Add(1)
			go func(consumer *Consumer) {
				defer wg.Done()
				consumer.Queue.Put(payload)
			}(c)
		}
		wg.Wait()
	}
}

//benchmark four consumers 1k payload
func (suite *BenchmarkSuite) BenchmarkFourCon1k(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < c.N; i++ {
		for _, c := range suite.mutliConsumer1k {
			wg.Add(1)
			go func(consumer *Consumer) {
				defer wg.Done()
				p, _ := consumer.Get()
				p.Ack()
			}(c)
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
			wg.Add(1)
			go func(consumer *Consumer) {
				defer wg.Done()
				consumer.Queue.Put(payload)
			}(c)
		}
		wg.Wait()
	}
}

//benchmark four consumers 4k payload
func (suite *BenchmarkSuite) BenchmarkFourCon4k(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < c.N; i++ {
		for _, c := range suite.mutliConsumer4k {
			wg.Add(1)
			go func(consumer *Consumer) {
				defer wg.Done()
				p, _ := consumer.Get()
				p.Ack()
			}(c)
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
			defer wg.Done()
			suite.queue1k.Put(payload)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, _ := suite.consumer1k.Get()
			p.Ack()
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
			defer wg.Done()
			suite.queue4k.Put(payload)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, _ := suite.consumer4k.Get()
			p.Ack()
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
			wg.Add(1)
			go func(consumer *Consumer) {
				defer wg.Done()
				consumer.Queue.Put(payload)
			}(c)
		}
		for _, c := range suite.mutliConsumer1k {
			wg.Add(1)
			go func(consumer *Consumer) {
				defer wg.Done()
				p, _ := consumer.Get()
				p.Ack()
			}(c)
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
			wg.Add(1)
			go func(consumer *Consumer) {
				defer wg.Done()
				consumer.Queue.Put(payload)
			}(c)
		}
		for _, c := range suite.mutliConsumer4k {
			wg.Add(1)
			go func(consumer *Consumer) {
				defer wg.Done()
				p, _ := consumer.Get()
				p.Ack()
			}(c)
		}
		wg.Wait()
	}
}

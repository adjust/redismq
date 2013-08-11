package main

import (
	//"fmt"
	"github.com/adeven/redismq"
	. "github.com/matttproud/gocheck"
	"strconv"
	"sync"
)

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
		p, _ := suite.consumer.Get()
		p.Ack()
	}
}

//benchmark single consumer multi 100 1k payload
func (suite *TestSuite) BenchmarkSingleConMulti1k(c *C) {
	payload := randomString(1024)
	for i := 0; i < 100000; i++ {
		suite.queue.Put(payload)
	}
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		p, _ := suite.consumer.MultiGet(100)
		p[99].MutliAck()
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
		p, _ := suite.consumer.Get()
		p.Ack()
	}
}

//benchmark four publishers 1k payload
func (suite *TestSuite) BenchmarkFourPub1k(c *C) {
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
			consumer, _ := suite.queue.AddConsumer("test" + strconv.Itoa(i) + strconv.Itoa(j))
			wg.Add(1)
			go func(consumer *redismq.Consumer) {
				p, _ := consumer.Get()
				p.Ack()
				defer wg.Done()
			}(consumer)
		}
		wg.Wait()
	}
}

//benchmark four publisher 4k payload
func (suite *TestSuite) BenchmarkFourPub4k(c *C) {
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
			consumer, _ := suite.queue.AddConsumer("test" + strconv.Itoa(i) + strconv.Itoa(j))
			wg.Add(1)
			go func(consumer *redismq.Consumer) {
				p, _ := consumer.Get()
				p.Ack()
				defer wg.Done()
			}(consumer)
		}
		wg.Wait()
	}
}

//benchmark single publisher and single consumer 1k payload
func (suite *TestSuite) BenchmarkSingPubSingCon1k(c *C) {
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
			p, _ := suite.consumer.Get()
			p.Ack()
			defer wg.Done()
		}()
		wg.Wait()
	}
}

//benchmark single publisher and single consumer 4k payload
func (suite *TestSuite) BenchmarkSingPubSingCon4k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024 * 4)
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
			p, _ := suite.consumer.Get()
			p.Ack()
			defer wg.Done()
		}()
		wg.Wait()
	}
}

//benchmark four publisher and four consumers 1k payload
func (suite *TestSuite) BenchmarkFourPubFourCon1k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024)
	//add first package so consumer doesn't wait for publisher
	suite.queue.Put(payload)
	for i := 0; i < c.N; i++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				suite.queue.Put(payload)
				defer wg.Done()
			}()
			consumer, _ := suite.queue.AddConsumer("test" + strconv.Itoa(i) + strconv.Itoa(j))
			wg.Add(1)
			go func(consumer *redismq.Consumer) {
				p, _ := consumer.Get()
				p.Ack()
				defer wg.Done()
			}(consumer)
		}
		wg.Wait()
	}
}

//benchmark four publisher and four consumers 4k payload
func (suite *TestSuite) BenchmarkFourPubFourCon4k(c *C) {
	var wg sync.WaitGroup
	payload := randomString(1024 * 4)
	//add first package so consumer doesn't wait for publisher
	suite.queue.Put(payload)
	for i := 0; i < c.N; i++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				suite.queue.Put(payload)
				defer wg.Done()
			}()
			consumer, _ := suite.queue.AddConsumer("test" + strconv.Itoa(i) + strconv.Itoa(j))
			wg.Add(1)
			go func(consumer *redismq.Consumer) {
				p, _ := consumer.Get()
				p.Ack()
				defer wg.Done()
			}(consumer)
		}
		wg.Wait()
	}
}

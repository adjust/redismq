package main

import (
	"github.com/adeven/redismq"
	"log"
	"math/rand"
	"runtime"
)

// This example demonstrates maximum performance
func main() {
	runtime.GOMAXPROCS(5)
	over := redismq.NewOverseer("localhost:6379", "", 9)
	server := redismq.NewServer("9999", over)
	go server.Start()
	testQueue, err := redismq.NewBufferedQueue("localhost:6379", "", 9, "example", 100)
	if err != nil {
		panic(err)
	}
	go write(testQueue)
	go read(testQueue, "1")
	go read(testQueue, "2")
	go read(testQueue, "3")
	go read(testQueue, "4")
	select {}
}

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

func write(queue *redismq.BufferedQueue) {
	payload := randomString(1024 * 1) //adjust for size
	for {
		queue.Put(payload)
	}
}

func read(queue *redismq.BufferedQueue, prefix string) {
	consumer, err := queue.AddConsumer("testconsumer" + prefix)
	if err != nil {
		panic(err)
	}
	consumer.ResetWorking()
	for {
		p, err := consumer.MultiGet(100)
		if err != nil {
			log.Println(err)
			continue
		}
		p[len(p)-1].MultiAck()
	}
}

package main

import (
	"log"
	"math/rand"
	"runtime"

	"github.com/adjust/redismq"
)

// This example demonstrates maximum performance
func main() {
	runtime.GOMAXPROCS(8)
	server := redismq.NewServer("localhost", "6379", "", 9, "9999")
	server.Start()
	testQueue := redismq.CreateBufferedQueue("localhost", "6379", "", 9, "example", 200)
	err := testQueue.Start()
	if err != nil {
		panic(err)
	}
	go write(testQueue)

	go read(testQueue, "1")
	go read(testQueue, "2")
	go read(testQueue, "3")
	go read(testQueue, "4")
	go read(testQueue, "5")
	go read(testQueue, "6")
	go read(testQueue, "7")
	go read(testQueue, "8")

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
		p, err := consumer.MultiGet(200)
		if err != nil {
			log.Println(err)
			continue
		}
		p[len(p)-1].MultiAck()
	}
}

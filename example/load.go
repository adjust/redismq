package main

import (
	"github.com/adeven/goenv"
	"github.com/adeven/redismq"
	"log"
	"math/rand"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(5)
	goenv := goenv.DefaultGoenv()
	over := redismq.NewOverseer(goenv)
	server := redismq.NewServer(goenv, over)
	go server.Start()
	go write("example")
	go read("example", "1")
	go read("example", "2")
	//go read("3")
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

func write(queue string) {
	goenv := goenv.DefaultGoenv()
	testQueue := redismq.NewQueue(goenv, queue)
	payload := randomString(1024 * 1) //adjust for size
	for {
		testQueue.Put(payload)
	}
}

func read(queue, prefix string) {
	goenv := goenv.DefaultGoenv()
	testQueue := redismq.NewQueue(goenv, queue)
	consumer, err := testQueue.AddConsumer("testconsumer" + prefix)
	if err != nil {
		panic(err)
	}
	consumer.ResetWorking()
	for {
		p, err := consumer.Get()
		if err != nil {
			log.Println(err)
			continue
		}
		err = p.Ack()
	}
}

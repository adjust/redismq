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
	go write()
	go read("1")
	go read("2")
	//go read("3")

	redismq.NewOverseer(goenv)
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

func write() {
	goenv := goenv.DefaultGoenv()
	testQueue := redismq.NewQueue(goenv, "example")
	payload := randomString(1024 * 1) //adjust for size
	for {
		testQueue.Put(payload)
	}
}

func read(prefix string) {
	goenv := goenv.DefaultGoenv()
	testQueue := redismq.NewQueue(goenv, "example")
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

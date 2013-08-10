package main

import (
	"github.com/adeven/goenv"
	"github.com/adeven/redismq"
	"math/rand"
)

func main() {
	goenv := goenv.DefaultGoenv()
	testQueue := redismq.NewQueue(goenv, "example")
	payload := randomString(1024 * 1) //adjust for size
	redismq.NewQueueWatcher(goenv)

	for {
		testQueue.Put(payload)
	}
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

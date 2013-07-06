package main

import (
	"fmt"
	"github.com/adeven/goenv"
	"github.com/adeven/rqueue"
)

func main() {
	goenv := goenv.DefaultGoenv()
	testQueue := rqueue.NewQueue(goenv, "clicks")
	for i := 0; i < 10; i++ {
		testQueue.Put("testpayload")
	}
	for i := 0; i < 10; i++ {
		p, err := testQueue.Get("testconsumer")
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println(p.CreatedAt)
		err = p.Ack()
		if err != nil {
			fmt.Println(err)
		}
	}
}

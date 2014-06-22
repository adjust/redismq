package main

import (
	"fmt"
	"github.com/adjust/redismq"
)

func main() {
	testQueue := redismq.CreateQueue("localhost", "6379", "", 9, "clicks")
	for i := 0; i < 10; i++ {
		testQueue.Put("testpayload")
	}
	consumer, err := testQueue.AddConsumer("testconsumer")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10; i++ {
		p, err := consumer.Get()
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

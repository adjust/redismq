package rqueue

func InputQueueName(queue *Queue) string {
	return "rqueue::" + queue.name
}

func WorkingQueueName(queue *Queue, consumer string) string {
	return "rqueue::" + queue.name + "::working::" + consumer
}

func FailedQueueName(queue *Queue) string {
	return "rqueue::" + queue.name + "::failed"
}

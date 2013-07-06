package rqueue

func InputQueueName(queue *Queue) string {
	return "rqueue::" + queue.name
}

func WorkingQueueName(queue *Queue, consumer string) string {
	return "requeue::" + queue.name + "::working::" + consumer
}

func FailedQueueName(queue *Queue) string {
	return "requeue::" + queue.name + "::failed"
}

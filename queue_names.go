package redismq

func InputQueueName(queue *Queue) string {
	return "redismq::" + queue.name
}

func WorkingQueueName(queue *Queue, consumer string) string {
	return "redismq::" + queue.name + "::working::" + consumer
}

func FailedQueueName(queue *Queue) string {
	return "redismq::" + queue.name + "::failed"
}

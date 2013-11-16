package redismq

func masterQueueKey() string {
	return "redismq::queues"
}

func queueWorkersKey(queue string) string {
	return queue + "::workers"
}

func queueInputKey(queue string) string {
	return "redismq::" + queue
}

func queueFailedKey(queue string) string {
	return "redismq::" + queue + "::failed"
}

func queueInputRateKey(queue string) string {
	return queueInputKey(queue) + "::rate"
}

func queueInputSizeKey(queue string) string {
	return queueInputKey(queue) + "::size"
}

func queueFailedSizeKey(queue string) string {
	return queueFailedKey(queue) + "::size"
}

func queueHeartbeatKey(queue string) string {
	return queueInputKey(queue) + "::buffered::heartbeat"
}

func queueWorkingPrefix(queue string) string {
	return "redismq::" + queue + "::working"
}

func consumerWorkingQueueKey(queue, consumer string) string {
	return queueWorkingPrefix(queue) + "::" + consumer
}
func consumerWorkingRateKey(queue, consumer string) string {
	return consumerWorkingQueueKey(queue, consumer) + "::rate"
}

func consumerHeartbeatKey(queue, consumer string) string {
	return consumerWorkingQueueKey(queue, consumer) + "::heartbeat"
}

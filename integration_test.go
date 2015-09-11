package redismq

import (
	"math/rand"
	"runtime"
	"testing"
	"time"

	. "github.com/matttproud/gocheck"
	"gopkg.in/redis.v3"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	queue       *Queue
	consumer    *Consumer
	redisClient *redis.Client
}

var (
	redisHost     = "localhost"
	redisPort     = "6379"
	redisPassword = ""
	redisDB       = int64(9)
	_             = Suite(&TestSuite{})
)

func (suite *TestSuite) SetUpSuite(c *C) {
	runtime.GOMAXPROCS(8)
	rand.Seed(time.Now().UTC().UnixNano())
	suite.redisClient = redis.NewClient(&redis.Options{
		Addr:     redisHost + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDB,
	})
}

func (suite *TestSuite) SetUpTest(c *C) {
	c.Assert(suite.redisClient.FlushDb().Err(), IsNil)
	suite.queue = CreateQueue(redisHost, redisPort, redisPassword, redisDB, "teststuff")
	suite.consumer, _ = suite.queue.AddConsumer("testconsumer")
}

// should not allow a second consumer with the same name
func (suite *TestSuite) TestUniqueConsumer(c *C) {
	_, err := suite.queue.AddConsumer("testconsumer")
	c.Check(err, Not(Equals), nil)
}

// should not select a non existing queue
func (suite *TestSuite) TestSelectQueue(c *C) {
	_, err := SelectQueue(redisHost, redisPort, redisPassword, redisDB, "i_dont_exists")
	c.Check(err, Not(Equals), nil)
	_, err = SelectBufferedQueue(redisHost, redisPort, redisPassword, redisDB, "me_neither", 200)
	c.Check(err, Not(Equals), nil)
}

// should put package into queue
func (suite *TestSuite) TestPutGetAndAck(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")
	c.Check(p.Ack(), Equals, nil)
	c.Check(suite.consumer.HasUnacked(), Equals, false)
}

// should queue packages
func (suite *TestSuite) TestQueuingPackages(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	c.Check(suite.queue.GetInputLength(), Equals, int64(100))
}

// shouldn't get 2nd package for consumer
func (suite *TestSuite) TestSecondGet(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)

	_, err = suite.consumer.Get()
	c.Check(err, Not(Equals), nil)

	err = p.Ack()
	c.Check(err, Equals, nil)

	_, err = suite.consumer.Get()
	c.Check(err, Equals, nil)
}

// test waiting for get
func (suite *TestSuite) TestWaitForGet(c *C) {
	go func() {
		p, err := suite.consumer.Get()
		c.Assert(err, Equals, nil)
		c.Check(p.Payload, Equals, "testpayload")
	}()
	time.Sleep(1 * time.Second)
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
}

// should get package for second consumer
func (suite *TestSuite) TestSecondConsumer(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")

	consumer, err := suite.queue.AddConsumer("testconsumer2")
	c.Assert(err, Equals, nil)
	consumer.ResetWorking()

	p2, err := consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload")
}

// should reject package to requeue
func (suite *TestSuite) TestRequeue(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Requeue(), Equals, nil)

	p2, err2 := suite.consumer.Get()
	c.Assert(err2, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload")
}

// should reject package to failed
func (suite *TestSuite) TestFailed(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Fail(), Equals, nil)

	c.Check(suite.queue.GetFailedLength(), Equals, int64(1))
}

// should get unacked package(s)
func (suite *TestSuite) TestGetUnacked(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	_, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	// assume that consumer crashed and receives err on get

	_, err = suite.consumer.Get()
	c.Assert(err, Not(Equals), nil)

	p, err := suite.consumer.GetUnacked()
	c.Assert(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")
	c.Check(p.Ack(), Equals, nil)

	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(0))
}

// should requeue failed
func (suite *TestSuite) TestRequeueFailed(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	for i := 0; i < 100; i++ {
		p, err := suite.consumer.Get()
		c.Check(err, Equals, nil)
		c.Check(p.Fail(), Equals, nil)
	}
	c.Check(suite.queue.GetFailedLength(), Equals, int64(100))
	c.Check(suite.queue.RequeueFailed(), Equals, nil)
	c.Check(suite.queue.GetFailedLength(), Equals, int64(0))
	c.Check(suite.queue.GetInputLength(), Equals, int64(100))
}

// should requeue working
func (suite *TestSuite) TestRequeueWorkingManual(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	_, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	// assume that consumer crashed and receives err on get

	_, err = suite.consumer.Get()
	c.Assert(err, Not(Equals), nil)

	p, err := suite.consumer.GetUnacked()
	c.Assert(err, Equals, nil)
	c.Check(p.Requeue(), Equals, nil)

	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(0))
	c.Check(suite.queue.GetInputLength(), Equals, int64(1))
}

// should requeue working
func (suite *TestSuite) TestRequeueWorking(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	_, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	// assume that consumer crashed and receives err on get

	_, err = suite.consumer.Get()
	c.Assert(err, Not(Equals), nil)

	err = suite.consumer.RequeueWorking()
	c.Assert(err, Equals, nil)

	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(0))
	c.Check(suite.queue.GetInputLength(), Equals, int64(1))
}

// should get failed
func (suite *TestSuite) TestGetFailed(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Fail(), Equals, nil)
	c.Check(suite.queue.GetFailedLength(), Equals, int64(1))

	p2, err := suite.consumer.GetFailed()
	c.Assert(err, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload")
	c.Check(p2.Ack(), Equals, nil)
	c.Check(suite.queue.GetFailedLength(), Equals, int64(0))
	c.Check(suite.queue.GetInputLength(), Equals, int64(0))

	// try getting smth from empty failed queue
	_, err = suite.consumer.GetFailed()
	c.Check(err, Not(Equals), nil)
}

// should handle multiple queues
func (suite *TestSuite) TestSecondQueue(c *C) {
	secondQueue := CreateQueue(redisHost, redisPort, redisPassword, redisDB, "teststuff2")
	secondConsumer, err := secondQueue.AddConsumer("testconsumer")
	c.Assert(err, Equals, nil)

	secondQueue.ResetInput()
	secondQueue.ResetFailed()
	secondConsumer.ResetWorking()
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	c.Check(secondQueue.Put("testpayload2"), Equals, nil)

	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Payload, Equals, "testpayload")

	p2, err := secondConsumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p2.Payload, Equals, "testpayload2")

	secondQueue.ResetInput()
	secondQueue.ResetFailed()
	secondConsumer.ResetWorking()
}

// should handle huge payloads
func (suite *TestSuite) TestHugePayload(c *C) {
	// 10MB payload
	payload := randomString(1024 * 1024 * 10)

	c.Check(suite.queue.Put(payload), Equals, nil)
	p, err := suite.consumer.Get()
	c.Assert(err, Equals, nil)
	c.Check(p.Payload, Equals, payload)
	c.Check(p.Ack(), Equals, nil)
	c.Check(suite.consumer.HasUnacked(), Equals, false)
}

// should get multiple packages from queue and ack all of them
func (suite *TestSuite) TestMultiGetAndAck(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	c.Check(suite.queue.GetInputLength(), Equals, int64(100))

	p, err := suite.consumer.MultiGet(100)
	c.Assert(err, Equals, nil)
	c.Check(suite.queue.GetInputLength(), Equals, int64(0))
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(100))

	for j := range p {
		c.Check(p[j].Payload, Equals, "testpayload")
	}
	c.Check(p[len(p)-1].MultiAck(), Equals, nil)
	c.Check(suite.consumer.HasUnacked(), Equals, false)
}

// should get multiple packages from queue and ack half of them
func (suite *TestSuite) TestMultiGetAndPartialAck(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	c.Check(suite.queue.GetInputLength(), Equals, int64(100))

	p, err := suite.consumer.MultiGet(100)
	c.Assert(err, Equals, nil)
	c.Check(suite.queue.GetInputLength(), Equals, int64(0))
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(100))

	for j := range p {
		c.Check(p[j].Payload, Equals, "testpayload")
	}
	c.Check(p[49].MultiAck(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(50))
}

// should get multiple packages from queue and not reject the middle one
func (suite *TestSuite) TestMultiGetAndBlockedReject(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	p, err := suite.consumer.MultiGet(100)
	c.Assert(err, Equals, nil)
	c.Check(p[49].Fail(), Not(Equals), nil)
	c.Check(p[48].MultiAck(), Equals, nil)
	c.Check(p[49].Fail(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(50))
}

// should get multiple packages from queue and ack them in one after another
func (suite *TestSuite) TestMultiGetAndMultiAck(c *C) {
	for i := 0; i < 100; i++ {
		c.Check(suite.queue.Put("testpayload"), Equals, nil)
	}
	p, err := suite.consumer.MultiGet(100)
	c.Assert(err, Equals, nil)
	c.Check(p[49].MultiAck(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(50))
	c.Check(p[49].MultiAck(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(50))
	c.Check(p[50].MultiAck(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(49))
	c.Check(p[98].MultiAck(), Equals, nil)
	c.Check(suite.consumer.GetUnackedLength(), Equals, int64(1))
}

// should not wait on multi get if less than requested amount of packages are in queue
func (suite *TestSuite) TestMultiGetNoWait(c *C) {
	c.Check(suite.queue.Put("testpayload"), Equals, nil)
	p, err := suite.consumer.MultiGet(100)
	c.Assert(err, Equals, nil)
	c.Check(p[len(p)-1].MultiAck(), Equals, nil)
	c.Check(suite.consumer.HasUnacked(), Equals, false)
}

// should not allow two buffered queues with the same name
func (suite *TestSuite) TestUniqueBufferedQueue(c *C) {
	q := CreateBufferedQueue(redisHost, redisPort, redisPassword, redisDB, "buffered_test1", 100)
	err := q.Start()
	c.Check(err, Equals, nil)

	q2 := CreateBufferedQueue(redisHost, redisPort, redisPassword, redisDB, "buffered_test1", 100)
	err = q2.Start()
	c.Check(err.Error(), Equals, "buffered queue with this name is already started")
}

// should be able to put and get from buffered queue
func (suite *TestSuite) TestBufferedQueue(c *C) {
	q := CreateBufferedQueue(redisHost, redisPort, redisPassword, redisDB, "buffered_test2", 100)
	err := q.Start()
	c.Assert(err, Equals, nil)

	for i := 0; i < 100; i++ {
		c.Check(q.Put("testpayload"), Equals, nil)
	}

	consumer, err := q.AddConsumer("testconsumer")
	c.Assert(err, Equals, nil)

	for i := 0; i < 100; i++ {
		p, err := consumer.Get()
		c.Check(err, Equals, nil)
		c.Check(p.Ack(), Equals, nil)
	}

	c.Check(q.GetInputLength(), Equals, int64(0))
	c.Check(consumer.GetUnackedLength(), Equals, int64(0))
}

// should not wait longer than 1 second to read from buffered queue
func (suite *TestSuite) TestBufferedQueueNoWait(c *C) {
	q := CreateBufferedQueue(redisHost, redisPort, redisPassword, redisDB, "buffered_test3", 100)
	err := q.Start()
	c.Assert(err, Equals, nil)

	c.Check(q.Put("testpayload"), Equals, nil)
	time.Sleep(1 * time.Second)
	consumer, err := q.AddConsumer("testconsumer")
	c.Assert(err, Equals, nil)

	p, err := consumer.Get()
	c.Check(err, Equals, nil)
	c.Check(p.Ack(), Equals, nil)

	c.Check(q.GetInputLength(), Equals, int64(0))
	c.Check(consumer.GetUnackedLength(), Equals, int64(0))
}

// should flush buffered queue upon call
func (suite *TestSuite) TestBufferedFlush(c *C) {
	q := CreateBufferedQueue(redisHost, redisPort, redisPassword, redisDB, "buffered_test4", 1000)
	err := q.Start()
	c.Assert(err, Equals, nil)
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 999; i++ {
		c.Check(q.Put("testpayload"), Equals, nil)
	}
	c.Check(len(q.Buffer), Not(Equals), 0)
	q.FlushBuffer()
	c.Check(len(q.Buffer), Equals, 0)
}

// TODO write stats watcher
// should get numbers of consumers

// should get publish rate for input

// should get consume rate for input

// should get consume rate for consumer

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

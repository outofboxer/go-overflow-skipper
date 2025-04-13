package skippingratelimiter

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"
)

type Message struct {
	ID      string
	Message string
}

func TestNewThrottle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	//throttle := NewIntThrottle(
	//	WithBufferCapacity ,
	//	WithBucketDuration[int](time.Second),
	//)

	throttler := NewThrottle[int](
		WithBufferCapacity(10),
		WithBucketDuration(2*time.Second),
	)

	throttler.messageHandler = func(msg Message) {
		fmt.Printf("got in handler: %+v\n", msg)
		time.Sleep(time.Millisecond * 100) // consumer has more sleep time than messageProducer.
	}
	throttler.skipMessageCallback = func(skippedMsgCount uint64) {
		fmt.Printf("skippedMsgCount: %d\n", skippedMsgCount)
	}

	throttler.messageProducer = func(ctx context.Context) (Message, error) {
		time.Sleep(time.Millisecond * 50) // producer awaken more frequently than messageHandler.
		return Message{
			ID:      "1",
			Message: "hello world",
		}, nil
	}

	err := throttler.Run(ctx)
	if err != nil {
		log.Fatalln(err)
	}
}

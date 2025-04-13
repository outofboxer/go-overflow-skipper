package skippingratelimiter

import (
	"context"
	"fmt"
	"time"
)

type Throttle[T any] struct {
	skippedMsgCount     uint64
	queue               chan T
	bucketDuration      time.Duration
	messageHandler      func(msg T)
	skipMessageCallback func(skippedMsgCount uint64)
	messageProducer     func(ctx context.Context) (T, error)
	lastBufferFill      time.Time
	bufferCapacity      int
}

type Option[T any] func(*Throttle[T])

func WithBufferCapacity[T any](capacity int) Option[T] {
	return func(t *Throttle[T]) {
		t.bufferCapacity = capacity
	}
}

func WithBucketDuration[T any](duration time.Duration) Option[T] {
	return func(t *Throttle[T]) {
		t.bucketDuration = duration
	}
}

// bufferCapacity int, bucketDuration time.Duration
func NewThrottle[T any](opts ...Option[T]) *Throttle[T] {
	t := &Throttle[T]{
		//queue:          make(chan T, bufferCapacity), // up to 'bufferCapacity' pending messages
		bufferCapacity: 100,         // default buffer capacity
		bucketDuration: time.Second, // default duration
	}

	for _, opt := range opts {
		opt(t)
	}

	t.queue = make(chan T, t.bufferCapacity)

	return t
}

func (t *Throttle[T]) Run(ctx context.Context) error {
	go t.messageProcessor(ctx)
	for {
		msg, err := t.messageProducer(ctx)
		if err != nil {
			if ctx.Err() == nil {
				return fmt.Errorf("reading messageHandler  failed, error: %w", err)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case t.queue <- msg:
		default:
			fmt.Println("dropping message")
			now := time.Now()
			t.skippedMsgCount++
			//  |now - 1sec|  Aster |now-0.1sec| => do nothing
			//  |now - 1sec|  Aster |now-2sec| => skip message
			if now.Add(-1 * t.bucketDuration).After(t.lastBufferFill) {
				t.skipMessageCallback(t.skippedMsgCount)
				t.skippedMsgCount = 0
				t.lastBufferFill = now
			}
		}
	}
}

func (t *Throttle[T]) messageProcessor(
	ctx context.Context,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-t.queue:
			if !ok {
				return
			}
			t.messageHandler(msg)
		}
	}
}

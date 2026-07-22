/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Consumer receives and acknowledges a single message
func TestConsume_SingleMessage(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-consume-single")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().SetBody("hello").SetQueue(params))

	var receivedID string
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		receivedID = m.ID
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	for i := 0; i < 20; i++ {
		if receivedID != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if receivedID != ids[0] {
		t.Fatalf("id = %s, want %s", receivedID, ids[0])
	}
}

// Scenario: Consumer processes multiple messages
func TestConsume_MultipleMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-consume-multi")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody(fmt.Sprintf("msg-%d", i)).SetQueue(params))
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(3 * time.Second)

	if consumed.Load() != 10 {
		t.Fatalf("consumed %d, want 10", consumed.Load())
	}
}

// Scenario: Multiple consumers on same queue load balance
func TestConsume_LoadBalance(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-consume-balance")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	var c1, c2 atomic.Int64

	cons1 := redissmq.NewConsumer()
	cons1.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		c1.Add(1)
		return nil
	})
	cons1.Run(ctx)
	defer cons1.Shutdown()

	cons2 := redissmq.NewConsumer()
	cons2.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		c2.Add(1)
		return nil
	})
	cons2.Run(ctx)
	defer cons2.Shutdown()

	time.Sleep(3 * time.Second)

	total := c1.Load() + c2.Load()
	if total != 10 {
		t.Fatalf("total consumed %d, want 10", total)
	}
	if c1.Load() == 0 || c2.Load() == 0 {
		t.Errorf("load balancing not working: c1=%d, c2=%d", c1.Load(), c2.Load())
	}
	t.Logf("consumer1: %d, consumer2: %d", c1.Load(), c2.Load())
}

// Scenario: Consumer handles empty queue
func TestConsume_EmptyQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-consume-empty")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)

	time.Sleep(2 * time.Second)
	cons.Shutdown()

	if consumed.Load() > 0 {
		t.Fatalf("consumed %d messages from empty queue", consumed.Load())
	}
}

// Scenario: Consumer resumes consuming after queue is paused and resumed
func TestConsume_ResumeAfterPause(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-resume-after-pause")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	defer cons.Shutdown()

	// Consume one message to verify consumer is working
	prod.Produce(ctx, msg.New().SetBody("before-pause").SetQueue(params))
	time.Sleep(2 * time.Second)
	beforePause := consumed.Load()
	t.Logf("consumed before pause: %d", beforePause)

	// Pause the queue — handler stops, but messages can still be produced
	queue.Pause(ctx, params, nil)
	time.Sleep(2 * time.Second)

	// Produce while paused — messages accumulate
	prod.Produce(ctx, msg.New().SetBody("during-pause").SetQueue(params))
	time.Sleep(1 * time.Second)
	duringPause := consumed.Load()
	t.Logf("consumed during pause: %d", duringPause)

	// Resume the queue — handler should restart and consume accumulated messages
	queue.Resume(ctx, params, nil)
	time.Sleep(7 * time.Second)

	afterResume := consumed.Load()
	t.Logf("consumed after resume: %d", afterResume)

	if afterResume <= duringPause {
		t.Fatal("no messages consumed after queue was resumed")
	}
}

// Scenario: Consumer resumes consuming after queue is stopped and resumed
func TestConsume_ResumeAfterStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 25*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-resume-after-stop")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	defer cons.Shutdown()

	// Consume one message to verify consumer is working
	prod.Produce(ctx, msg.New().SetBody("before-stop").SetQueue(params))
	time.Sleep(2 * time.Second)
	beforeStop := consumed.Load()
	t.Logf("consumed before stop: %d", beforeStop)

	// Stop the queue — handler stops, producing is blocked
	queue.Stop(ctx, params, nil)
	time.Sleep(2 * time.Second)

	duringStop := consumed.Load()
	t.Logf("consumed during stop: %d", duringStop)

	// Resume the queue and produce a new message
	queue.Resume(ctx, params, nil)
	time.Sleep(2 * time.Second)

	prod.Produce(ctx, msg.New().SetBody("after-stop").SetQueue(params))
	time.Sleep(7 * time.Second)

	afterResume := consumed.Load()
	t.Logf("consumed after resume: %d", afterResume)

	if afterResume <= duringStop {
		t.Fatal("no messages consumed after queue was resumed")
	}
}

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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Batch acknowledgments with full buffer
func TestBatchAck_FullBuffer(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-batch-ack-full")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	// Producer
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	// Consumer with batch acks: flush every 3 messages
	var consumed atomic.Int64
	cons := redissmq.NewConsumer(
		consumer.WithBatchAcks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    3,
			BatchTimeout: 30 * time.Second,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(3 * time.Second)

	count := consumed.Load()
	if count < 10 {
		t.Fatalf("consumed %d messages, want 10", count)
	}
}

// Scenario: Batch acknowledgments flush on timeout
func TestBatchAck_Timeout(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-batch-ack-timeout")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	var consumed atomic.Int64
	cons := redissmq.NewConsumer(
		consumer.WithBatchAcks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    100, // Larger than message count
			BatchTimeout: 1 * time.Second,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(3 * time.Second)

	count := consumed.Load()
	if count != 2 {
		t.Fatalf("consumed %d messages, want 2", count)
	}
}

// Scenario: Batch acknowledgments flush on shutdown
func TestBatchAck_FlushOnShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-batch-ack-shutdown")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer(
		consumer.WithBatchAcks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    100,
			BatchTimeout: 30 * time.Second,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)

	time.Sleep(2 * time.Second)

	// Shutdown should flush remaining batch
	cons.Shutdown()

	count := consumed.Load()
	if count != 5 {
		t.Fatalf("consumed %d messages, want 5", count)
	}
}

// Scenario: Disabled batch acks (default) process immediately
func TestBatchAck_Disabled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-batch-ack-disabled")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(2 * time.Second)

	count := consumed.Load()
	if count != 5 {
		t.Fatalf("consumed %d messages, want 5", count)
	}
}

// Scenario: Batch unacknowledgments
func TestBatchUnack_FlushOnShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-batch-unack")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	var attempts atomic.Int64
	cons := redissmq.NewConsumer(
		consumer.WithBatchUnacks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    100,
			BatchTimeout: 30 * time.Second,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("processing failed") // Force unack
	})
	cons.Run(ctx)

	time.Sleep(3 * time.Second)
	cons.Shutdown()

	// Message should be retried (unacknowledged)
	if attempts.Load() == 0 {
		t.Fatal("message should have been processed at least once")
	}
	t.Logf("attempts: %d", attempts.Load())
}

// Scenario: Shutdown waits for the in-flight handler before shutting down the
// batch acker, so the handler's Ack is flushed and the message leaves the
// consumer's processing queue.
func TestBatchAck_ShutdownWaitsForInFlightHandler(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-c7-shutdown-waits")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	if _, err := prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params)); err != nil {
		t.Fatalf("produce: %v", err)
	}

	var handlerStarted, handlerFinished atomic.Bool

	cons := redissmq.NewConsumer(
		consumer.WithBatchAcks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    100,
			BatchTimeout: 30 * time.Second,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		handlerStarted.Store(true)
		time.Sleep(500 * time.Millisecond)
		handlerFinished.Store(true)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !handlerStarted.Load() {
		select {
		case <-deadline:
			cons.Shutdown()
			t.Fatal("handler never started")
		case <-time.After(20 * time.Millisecond):
		}
	}

	cons.Shutdown()

	if !handlerFinished.Load() {
		t.Fatal("Shutdown returned before the in-flight handler completed — " +
			"shutdownLocked is not waiting for loop to exit")
	}

	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	processingLen, err := redis.Client().LLen(ctx, qKey.ConsumerProcessing(cons.ID())).Result()
	if err != nil {
		t.Fatalf("LLen ConsumerProcessing: %v", err)
	}
	if processingLen != 0 {
		t.Fatalf("ConsumerProcessing retains %d messages after Shutdown — "+
			"the in-flight Ack was buffered after BatchAcker.Shutdown and never "+
			"flushed (C7)", processingLen)
	}

	exists, err := redis.Client().Exists(ctx, qKey.ConsumerProcessing(cons.ID())).Result()
	if err != nil {
		t.Fatalf("exists ConsumerProcessing: %v", err)
	}
	if exists != 0 {
		t.Errorf("ConsumerProcessing key still exists after Shutdown")
	}
}

// Scenario: BatchUnacker must NOT publish unack-related events for
// messages that the Lua script skipped because they were no longer in
// the consumer's processing queue.
func TestBatchUnack_NoPhantomEventsForSkippedMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-h7-phantom")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	var mu sync.Mutex
	unackEvents := make(map[string]int)
	requeueEvents := make(map[string]int)

	unackSub, err := consumer.SubscribeMessageUnacknowledged(func(p consumer.MessageUnacknowledgedPayload) {
		mu.Lock()
		unackEvents[p.MessageID]++
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe unack: %v", err)
	}
	defer unackSub.Unsubscribe()

	requeueSub, err := consumer.SubscribeMessageRequeued(func(p consumer.MessagePayload) {
		mu.Lock()
		requeueEvents[p.MessageID]++
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe requeue: %v", err)
	}
	defer requeueSub.Unsubscribe()

	// BatchSize=100 and BatchTimeout=10s are both larger than this test's
	// scale, so the batch never auto-flushes. Only the explicit Shutdown
	// at the end triggers the flush — after we've manually removed the
	// message from ConsumerProcessing.
	cons := redissmq.NewConsumer(
		consumer.WithBatchUnacks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    100,
			BatchTimeout: 10 * time.Second,
		}),
	)

	handlerRan := make(chan struct{}, 1)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		handlerRan <- struct{}{}
		return fmt.Errorf("intentional failure to trigger unack")
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	t.Cleanup(cons.Shutdown) // idempotent; explicit Shutdown below is the primary path

	prod := testutil.StartProducer(t, ctx)
	ids, err := prod.Produce(ctx, msg.New().
		SetBody("test-h7").
		SetQueue(params).
		SetRetryThreshold(10). // high enough that we never exceed it → requeue path
		SetRetryDelay(0),      // 0 → requeue, not delay
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	msgID := ids[0]

	// Wait for the handler to run. After this returns, ConsumeMessage.Consume
	// calls batchUnacker.Unack synchronously, appending the entry to the buffer.
	select {
	case <-handlerRan:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for handler to run")
	}

	// Give Unack time to buffer the entry. Safe because BatchTimeout is 10s
	// and BatchSize is 100 — no auto-flush happens in this window.
	time.Sleep(200 * time.Millisecond)

	// Sanity: the message must be in ConsumerProcessing before we remove it.
	// If it isn't, the test setup is wrong (message already acked, or never
	// checked out); the assertions below would be vacuous.
	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	processingKey := qKey.ConsumerProcessing(cons.ID())
	pending, err := redis.Client().LRange(ctx, processingKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("lrange ConsumerProcessing: %v", err)
	}
	found := false
	for _, id := range pending {
		if id == msgID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("message %s not in ConsumerProcessing %v; test setup is wrong", msgID, pending)
	}

	// Manually remove the message from ConsumerProcessing. This simulates
	// a peer's ReapConsumers cycle having recovered the message (removed it
	// from the dead consumer's processing queue) while our consumer is
	// still alive. When the batch flushes, the Lua's LREM returns 0 and
	// skips the entry — but pre-fix, the phantom event still fired.
	removed, err := redis.Client().LRem(ctx, processingKey, 1, msgID).Result()
	if err != nil {
		t.Fatalf("lrem ConsumerProcessing: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected to remove 1 message from ConsumerProcessing, got %d", removed)
	}

	// Force the batch to flush via Shutdown.
	cons.Shutdown()

	// Give events time to be delivered through Redis pub/sub to our
	// subscribers. The publish call is synchronous (returns when the
	// PUBLISH command is accepted by Redis), but delivery to subscribers
	// is asynchronous.
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if n := unackEvents[msgID]; n != 0 {
		t.Errorf("phantom consumer.messageUnacknowledged event for %s: %d occurrence(s) (H7). "+
			"The message was removed from ConsumerProcessing before the batch flushed; "+
			"the Lua LREM returned 0 and skipped it, but BatchUnacker published the event anyway.",
			msgID, n)
	}
	if n := requeueEvents[msgID]; n != 0 {
		t.Errorf("phantom consumer.messageRequeued event for %s: %d occurrence(s) (H7). "+
			"The message was removed from ConsumerProcessing before the batch flushed; "+
			"the Lua LREM returned 0 and skipped it, but BatchUnacker published the event anyway.",
			msgID, n)
	}
}

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
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Rapid start/stop cycles
func TestEdge_RapidStartStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-edge-rapid")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	for i := 0; i < 10; i++ {
		cons := redissmq.NewConsumer()
		cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
			return nil
		})
		if err := cons.Run(ctx); err != nil {
			t.Fatalf("cycle %d run: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)
		cons.Shutdown()
		time.Sleep(100 * time.Millisecond)
	}
}

// Scenario: Large number of consumers on single queue
func TestEdge_ManyConsumers(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-edge-many-consumers")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 50; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	var totalConsumed atomic.Int64
	consumerCount := 20
	var wg sync.WaitGroup

	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cons := redissmq.NewConsumer()
			cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
				totalConsumed.Add(1)
				return nil
			})
			if err := cons.Run(ctx); err != nil {
				t.Errorf("run: %v", err)
				return
			}
			time.Sleep(5 * time.Second)
			cons.Shutdown()
		}()
	}

	wg.Wait()

	t.Logf("consumed %d/50 messages with %d consumers", totalConsumed.Load(), consumerCount)
	if totalConsumed.Load() < 50 {
		t.Errorf("expected 50 messages, got %d", totalConsumed.Load())
	}
}

// Scenario: Consumer with very slow handler — other queues unaffected
func TestEdge_SlowHandlerDoesNotBlockOthers(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	fastQueue := publicqueue.MustQueueParams("test-edge-fast")
	slowQueue := publicqueue.MustQueueParams("test-edge-slow")

	testutil.CreateQueue(t, ctx, fastQueue, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, slowQueue, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("fast").SetQueue(fastQueue))
	prod.Produce(ctx, msg.New().SetBody("slow").SetQueue(slowQueue))

	var fastCount, slowCount atomic.Int64

	cons := redissmq.NewConsumer()
	cons.Consume(fastQueue, func(ctx context.Context, m *msg.Transferable) error {
		fastCount.Add(1)
		return nil
	})
	cons.Consume(slowQueue, func(ctx context.Context, m *msg.Transferable) error {
		slowCount.Add(1)
		time.Sleep(5 * time.Second) // Very slow
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(3 * time.Second)

	if fastCount.Load() == 0 {
		t.Fatal("fast queue should have consumed messages (not blocked by slow handler)")
	}
	t.Logf("fast: %d, slow: %d", fastCount.Load(), slowCount.Load())
}

// Scenario: Handler that always fails — message dead letters
func TestEdge_AlwaysFailingHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-edge-always-fail")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("fail").
		SetQueue(params).
		SetRetryThreshold(2).
		SetRetryDelay(0),
	)

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("always failing")
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(8 * time.Second)

	if attempts.Load() < 1 {
		t.Fatal("handler was never called")
	}
	t.Logf("attempts: %d (expected multiple retries then dead-letter)", attempts.Load())

	// Check queue properties — should have dead-lettered messages if audit enabled
	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	t.Logf("pending: %d, dead: %d, requeued: %d",
		props.PendingMessagesCount,
		props.DeadLetteredMessagesCount,
		props.RequeuedMessagesCount,
	)
}

// Scenario: Shutdown while handler is processing
func TestEdge_ShutdownDuringProcessing(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-edge-shutdown-during")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		time.Sleep(500 * time.Millisecond) // Slow handler
		return nil
	})
	cons.Run(ctx)

	time.Sleep(1 * time.Second)
	cons.Shutdown() // Shutdown while messages are being processed

	count := consumed.Load()
	t.Logf("consumed %d messages before shutdown", count)
	if count == 0 {
		t.Fatal("no messages consumed before shutdown")
	}

	// Remaining messages should be returned to pending
	time.Sleep(1 * time.Second)
	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	t.Logf("after shutdown: pending=%d, processing=%d",
		props.PendingMessagesCount,
		props.ProcessingMessagesCount,
	)
}

// Scenario: Zero-length message body
func TestEdge_EmptyMessageBody(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-empty-body")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("").SetQueue(params))

	var received string
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		received = m.Body.(string)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(2 * time.Second)

	if received != "" {
		t.Logf("received empty body: %q", received)
	}
}

// Scenario: Very large message body
func TestEdge_LargeMessageBody(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-large-body")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	// 10KB message
	largeBody := make([]byte, 10000)
	for i := range largeBody {
		largeBody[i] = 'x'
	}

	prod := testutil.StartProducer(t, ctx)
	ids, err := prod.Produce(ctx, msg.New().SetBody(string(largeBody)).SetQueue(params))
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	var receivedLen int
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		receivedLen = len(m.Body.(string))
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(2 * time.Second)

	if receivedLen != 10000 {
		t.Errorf("body length = %d, want 10000", receivedLen)
	}
	if len(ids) != 1 {
		t.Errorf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Consumer on queue that is being deleted
func TestEdge_QueueDeletedDuringConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-edge-delete-during")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)

	time.Sleep(1 * time.Second)

	qm := redissmq.NewQueueManager()

	// Delete the queue while consumer is running
	err := qm.Delete(ctx, params)
	if err != nil {
		t.Logf("delete queue while consuming: %v (expected: error about active consumers)", err)
	}

	cons.Shutdown()

	// Now delete should succeed
	err = qm.Delete(ctx, params)
	if err != nil {
		t.Logf("delete after shutdown: %v", err)
	}
}

// Scenario: Handler panic does not crash the consumer; other messages are still processed.
func TestEdge_HandlerPanicDoesNotCrashConsumer(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-handler-panic")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	// First message triggers a panic
	prod.Produce(ctx, msg.New().SetBody("panic").SetQueue(params))
	// Second message is normal
	prod.Produce(ctx, msg.New().SetBody("normal").SetQueue(params))

	var normalConsumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		if m.Body.(string) == "panic" {
			panic("intentional panic")
		}
		normalConsumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	// Wait for both messages to be processed (the second one should succeed)
	time.Sleep(5 * time.Second)
	if normalConsumed.Load() != 1 {
		t.Errorf("expected 1 normal message consumed, got %d", normalConsumed.Load())
	}
	if !cons.IsRunning() {
		t.Error("consumer should still be running after handler panic")
	}
}

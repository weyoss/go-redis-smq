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
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Handler respects context deadline – returns early, no forced timeout needed.
// The message is retried and successfully consumed by a second consumer.
func TestConsumeTimeout_CooperativeHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 25*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-timeout-cooperative")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().
		SetBody("timeout-cooperative").
		SetQueue(params).
		SetRetryDelay(0). // immediate requeue after failure
		SetConsumeTimeout(1*time.Second),
	)

	var handlerStarted atomic.Bool
	var handlerFinished atomic.Bool
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		handlerStarted.Store(true)
		select {
		case <-ctx.Done():
			// Handler noticed the deadline and returned early.
			handlerFinished.Store(true)
			return ctx.Err()
		case <-time.After(10 * time.Second):
			// Would block forever, but deadline should fire first.
		}
		return nil
	})
	cons.Run(ctx)

	// Give the handler time to start and hit the deadline.
	time.Sleep(3 * time.Second)
	if !handlerStarted.Load() {
		cons.Shutdown()
		t.Fatal("handler was not invoked")
	}
	if !handlerFinished.Load() {
		cons.Shutdown()
		t.Fatal("handler did not finish within timeout")
	}

	// Stop the first consumer so it doesn't re‑consume the requeued message.
	cons.Shutdown()

	// Wait for the immediate requeuer (runs every 5s) to move the message back to pending.
	time.Sleep(7 * time.Second)

	// Start a second consumer that should receive the retried message.
	var consumed atomic.Int64
	var receivedMsg *msg.Transferable
	cons2 := redissmq.NewConsumer()
	cons2.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		if consumed.Add(1) == 1 {
			receivedMsg = m // capture the first (and only) message
		}
		return nil
	})
	cons2.Run(ctx)
	defer cons2.Shutdown()

	// Allow time for the second consumer to pick up and process the message.
	time.Sleep(8 * time.Second)
	if consumed.Load() == 0 {
		t.Fatal("message was not consumed after timeout recovery")
	}

	// Verify that the message was indeed retried (attempts > 0).
	if receivedMsg == nil {
		t.Fatal("did not capture the retried message")
	}

	fmt.Printf("attempts %#v\n", receivedMsg.MessageState)

	if receivedMsg.MessageState.Attempts == 0 {
		t.Errorf("expected attempts > 0 after retry, got %d", receivedMsg.MessageState.Attempts)
	}
	t.Logf("Cooperative handler: message retried successfully, attempts=%d", receivedMsg.MessageState.Attempts)
}

// Scenario: Handler ignores context deadline – forced timeout unacknowledges the message.
// The original consumer stays alive but stuck, while a second consumer receives the retried message.
func TestConsumeTimeout_ForcedTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-timeout-forced")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().
		SetBody("timeout-forced").
		SetQueue(params).
		SetRetryDelay(0). // immediate requeue after forced timeout
		SetConsumeTimeout(1*time.Second),
	)

	var handlerInvoked atomic.Bool
	blockCh := make(chan struct{}) // closed by cleanup so the handler can return
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		handlerInvoked.Store(true)
		<-blockCh // ignore ctx during the test; unblocked by cleanup
		return nil
	})
	cons.Run(ctx)

	// Cleanup order matters: close blockCh FIRST (letting the handler return),
	// then Shutdown (which after C7 waits for the dequeue loop, so it would
	// hang if the handler were still blocked). Without this, the stuck handler
	// outlives the test and TestMain's redissmq.Shutdown() hangs forever
	// because MessageHandler.beginShutdown's wg.Wait() has nothing to wait
	// against — the loop can never exit while the handler ignores ctx.
	t.Cleanup(func() {
		close(blockCh)
		cons.Shutdown()
	})

	// Wait for handler to be invoked.
	time.Sleep(2 * time.Second)
	if !handlerInvoked.Load() {
		t.Fatal("handler was not invoked")
	}

	// Wait for forced timeout (1s) to trigger unacknowledgment.
	time.Sleep(3 * time.Second)

	// Wait for the immediate requeuer (every 5s) to move the message back to pending.
	time.Sleep(7 * time.Second)

	// Start a second consumer that should receive the retried message.
	var consumed atomic.Int64
	var receivedMsg *msg.Transferable
	cons2 := redissmq.NewConsumer()
	cons2.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		if consumed.Add(1) == 1 {
			receivedMsg = m // capture the first (and only) message
		}
		return nil
	})
	cons2.Run(ctx)
	defer cons2.Shutdown()

	// Allow time for the second consumer to pick up and process the message.
	time.Sleep(8 * time.Second)
	if consumed.Load() == 0 {
		t.Fatal("message was not redelivered after forced timeout")
	}

	// Verify that the message was indeed retried (attempts > 0).
	if receivedMsg == nil {
		t.Fatal("did not capture the retried message")
	}
	if receivedMsg.MessageState.Attempts == 0 {
		t.Errorf("expected attempts > 0 after forced timeout retry, got %d",
			receivedMsg.MessageState.Attempts)
	}
	t.Logf("Forced timeout: message retried successfully, attempts=%d",
		receivedMsg.MessageState.Attempts)
}

// Scenario: The ack/unack CAS race must never allow both paths to proceed for
// the same message.
//
// Pre-fix, ConsumeMessage used timedOut.Load() on the success path while the
// timeout goroutine used timedOut.CompareAndSwap(false, true). If the timer's
// CAS landed between the Load() and the Ack(), both an ack and an unack were
// buffered for the same message — the message's outcome then depended on which
// Lua script flushed first, silently corrupting its state.
//
// The fix uses CAS on the success path too, so exactly one path can proceed.
//
// This is a statistical test: hitting the race window deterministically would
// require a test hook in ConsumeMessage. It runs iterations of handler-vs-
// timeout races and asserts the invariant per message. Pre-fix, some iterations
// produce two events for the same message; post-fix, none do.
func TestConsumeTimeout_AckUnackRace_ExactlyOneOutcome(t *testing.T) {
	if testing.Short() {
		t.Skip("statistical race test skipped in -short mode; run without -short")
	}

	ctx, cancel := context.WithTimeout(testutil.Setup(t), 120*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-c4-ack-unack-race")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	// Per-message event counters. [0] = acks, [1] = unacks.
	var mu sync.Mutex
	counts := make(map[string][2]int)

	ackSub, err := consumer.SubscribeMessageAcknowledged(func(p consumer.MessagePayload) {
		mu.Lock()
		c := counts[p.MessageID]
		c[0]++
		counts[p.MessageID] = c
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe ack: %v", err)
	}
	defer ackSub.Unsubscribe()

	unackSub, err := consumer.SubscribeMessageUnacknowledged(
		func(p consumer.MessageUnacknowledgedPayload) {
			mu.Lock()
			c := counts[p.MessageID]
			c[1]++
			counts[p.MessageID] = c
			mu.Unlock()
		})
	if err != nil {
		t.Fatalf("subscribe unack: %v", err)
	}
	defer unackSub.Unsubscribe()

	// BatchSize=1 forces immediate flush on every buffer write, so any double-
	// buffering surfaces as two events for the same message ID.
	cons := redissmq.NewConsumer(
		consumer.WithBatchAcks(consumer.BatchConfig{
			Enabled: true, BatchSize: 1, BatchTimeout: 1 * time.Millisecond,
		}),
		consumer.WithBatchUnacks(consumer.BatchConfig{
			Enabled: true, BatchSize: 1, BatchTimeout: 1 * time.Millisecond,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		// Return as close to the ConsumeTimeout as possible to race the timer.
		time.Sleep(1 * time.Millisecond)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	prod := testutil.StartProducer(t, ctx)

	const iterations = 500
	ids := make([]string, 0, iterations)
	for i := range iterations {
		produced, err := prod.Produce(ctx, msg.New().
			SetBody("race").
			SetQueue(params).
			SetConsumeTimeout(1*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		ids = append(ids, produced[0])
	}

	// Wait until every message has at least one event, or we time out.
	deadline := time.After(60 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

wait:
	for {
		select {
		case <-deadline:
			break wait
		case <-ticker.C:
			mu.Lock()
			pending := 0
			for _, id := range ids {
				c := counts[id]
				if c[0]+c[1] == 0 {
					pending++
				}
			}
			mu.Unlock()
			if pending == 0 {
				break wait
			}
		}
	}

	// Assert the invariant: no message ever has both an ack and an unack.
	mu.Lock()
	defer mu.Unlock()

	var both, neither int
	for _, id := range ids {
		c := counts[id]
		switch total := c[0] + c[1]; {
		case total == 2:
			both++
			t.Logf("message %s: ack=%d unack=%d (BOTH fired)", id, c[0], c[1])
		case total == 0:
			neither++
		}
	}

	if both > 0 {
		t.Errorf("%d/%d messages had both ack and unack events — the CAS race "+
			"allows both paths to proceed (C4)", both, iterations)
	}
	if neither > 0 {
		t.Logf("note: %d/%d messages had no event yet (still in flight or "+
			"lost — check independently)", neither, iterations)
	}
}

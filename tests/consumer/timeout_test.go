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
	blockCh := make(chan struct{}) // never closed, handler blocks forever
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		handlerInvoked.Store(true)
		<-blockCh // ignore context and block forever
		return nil
	})
	cons.Run(ctx)
	// The handler is stuck, so the consumer's dequeue loop cannot fetch new messages.
	// We intentionally do NOT shut down this consumer.

	// Wait for handler to be invoked.
	time.Sleep(2 * time.Second)
	if !handlerInvoked.Load() {
		cons.Shutdown()
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
		t.Errorf("expected attempts > 0 after forced timeout retry, got %d", receivedMsg.MessageState.Attempts)
	}
	t.Logf("Forced timeout: message retried successfully, attempts=%d", receivedMsg.MessageState.Attempts)
}

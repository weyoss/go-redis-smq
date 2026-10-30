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
		SetRetryDelay(0).
		SetConsumeTimeout(1*time.Second),
	)

	var handlerStarted, handlerFinished atomic.Bool
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		handlerStarted.Store(true)
		select {
		case <-ctx.Done():
			handlerFinished.Store(true)
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}
		return nil
	})
	cons.Run(ctx)

	time.Sleep(3 * time.Second)
	if !handlerStarted.Load() {
		cons.Shutdown()
		t.Fatal("handler was not invoked")
	}
	if !handlerFinished.Load() {
		cons.Shutdown()
		t.Fatal("handler did not finish within timeout")
	}

	cons.Shutdown()
	time.Sleep(7 * time.Second)

	receivedMsgCh := make(chan *msg.Transferable, 1)
	var consumed atomic.Int64
	cons2 := redissmq.NewConsumer()
	cons2.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		if consumed.Add(1) == 1 {
			receivedMsgCh <- m
		}
		return nil
	})
	cons2.Run(ctx)
	defer cons2.Shutdown()

	var receivedMsg *msg.Transferable
	select {
	case receivedMsg = <-receivedMsgCh:
	case <-time.After(8 * time.Second):
		t.Fatal("message was not consumed after timeout recovery")
	}
	if consumed.Load() == 0 {
		t.Fatal("message was not consumed after timeout recovery")
	}
	if receivedMsg.MessageState.Attempts == 0 {
		t.Errorf("expected attempts > 0 after retry, got %d", receivedMsg.MessageState.Attempts)
	}
	t.Logf("Cooperative handler: message retried successfully, attempts=%d", receivedMsg.MessageState.Attempts)
}

// Scenario: Handler ignores context deadline – forced timeout unacknowledges the message.
// The original consumer is stuck during the test (exiting only after cleanup); a second consumer receives the retried message.
func TestConsumeTimeout_ForcedTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-timeout-forced")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().
		SetBody("timeout-forced").
		SetQueue(params).
		SetRetryDelay(0).
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
	// then Shutdown (which after the C7 fix waits for the dequeue loop, so it
	// would hang if the handler were still blocked). Without this, the stuck
	// handler outlives the test and TestMain's redissmq.Shutdown() hangs.
	t.Cleanup(func() {
		close(blockCh)
		cons.Shutdown()
	})

	time.Sleep(2 * time.Second)
	if !handlerInvoked.Load() {
		t.Fatal("handler was not invoked")
	}

	time.Sleep(3 * time.Second)
	time.Sleep(7 * time.Second)

	receivedMsgCh := make(chan *msg.Transferable, 1)
	var consumed atomic.Int64
	cons2 := redissmq.NewConsumer()
	cons2.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		if consumed.Add(1) == 1 {
			receivedMsgCh <- m
		}
		return nil
	})
	cons2.Run(ctx)
	defer cons2.Shutdown()

	var receivedMsg *msg.Transferable
	select {
	case receivedMsg = <-receivedMsgCh:
	case <-time.After(8 * time.Second):
		t.Fatal("message was not redelivered after forced timeout")
	}
	if consumed.Load() == 0 {
		t.Fatal("message was not redelivered after forced timeout")
	}
	if receivedMsg.MessageState.Attempts == 0 {
		t.Errorf("expected attempts > 0 after forced timeout retry, got %d",
			receivedMsg.MessageState.Attempts)
	}
	t.Logf("Forced timeout: message retried successfully, attempts=%d",
		receivedMsg.MessageState.Attempts)
}

// Scenario: The ack/unack CAS race must never allow both paths to proceed
// for the same message.
//
// Regression test for the bug where ConsumeMessage used timedOut.Load() on
// the success path while the timeout goroutine used CompareAndSwap. If the
// timer's CAS landed between the Load() and the Ack(), both an ack and an
// unack were buffered for the same message — outcome nondeterministic.
func TestConsumeTimeout_AckUnackRace_ExactlyOneOutcome(t *testing.T) {
	if testing.Short() {
		t.Skip("statistical race test skipped in -short mode")
	}

	ctx, cancel := context.WithTimeout(testutil.Setup(t), 120*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-c4-ack-unack-race")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

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

	cons := redissmq.NewConsumer(
		consumer.WithBatchAcks(consumer.BatchConfig{
			Enabled: true, BatchSize: 1, BatchTimeout: 1 * time.Millisecond,
		}),
		consumer.WithBatchUnacks(consumer.BatchConfig{
			Enabled: true, BatchSize: 1, BatchTimeout: 1 * time.Millisecond,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
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
	for i := 0; i < iterations; i++ {
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
		t.Logf("note: %d/%d messages had no event yet", neither, iterations)
	}
}

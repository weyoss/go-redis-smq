/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer_test

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicproducer "github.com/weyoss/go-redis-smq/pkg/producer"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Produce a single message to a queue
func TestProduce_SingleMessage(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-produce-single")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("hello").SetQueue(params)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
	if ids[0] == "" {
		t.Fatal("expected non-empty message ID")
	}
}

// Scenario: Produce multiple messages
func TestProduce_MultipleMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-produce-multi")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	var allIDs []string
	for i := 0; i < 10; i++ {
		m := msg.New().SetBody("msg").SetQueue(params)
		ids, err := prod.Produce(ctx, m)
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		allIDs = append(allIDs, ids[0])
	}

	if len(allIDs) != 10 {
		t.Fatalf("expected 10 IDs, got %d", len(allIDs))
	}

	// Verify queue has 10 messages
	props, err := redissmq.NewQueueManager().Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.MessagesCount != 10 {
		t.Fatalf("messages = %d, want 10", props.MessagesCount)
	}
	if props.PendingMessagesCount != 10 {
		t.Fatalf("pending = %d, want 10", props.PendingMessagesCount)
	}
}

// Scenario: Produce to non-existent queue
func TestProduce_NonExistentQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("nonexistent")
	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error for non-existent queue")
	}
}

// Scenario: Produce without starting producer
func TestProduce_NotRunning(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-produce-not-running")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := redissmq.NewProducer()

	m := msg.New().SetBody("msg").SetQueue(params)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error when producer not running")
	}
}

// Scenario: Produce with TTL
func TestProduce_WithTTL(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-produce-ttl")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("ttl-msg").
		SetQueue(params).
		SetTTL(5 * time.Minute).
		SetRetryThreshold(3).
		SetRetryDelay(10 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Produce scheduled message
func TestProduce_Scheduled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-produce-scheduled")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Delayed message
	m := msg.New().
		SetBody("scheduled").
		SetQueue(params).
		SetScheduledDelay(1 * time.Hour)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce delayed: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Verify it appears in scheduled, not pending
	props, err := redissmq.NewQueueManager().Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
	if props.PendingMessagesCount != 0 {
		t.Errorf("pending = %d, want 0", props.PendingMessagesCount)
	}
}

// Scenario: Produce to stopped queue fails
func TestProduce_StoppedQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-produce-stopped")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	redissmq.NewStateManager().Stop(ctx, params, nil)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error: cannot produce to stopped queue")
	}
}

// Scenario: Produce to paused queue succeeds
func TestProduce_PausedQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-produce-paused")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	redissmq.NewStateManager().Pause(ctx, params, nil)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce to paused queue: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Messages accumulate in pending
	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	if props.PendingMessagesCount != 1 {
		t.Errorf("pending = %d, want 1", props.PendingMessagesCount)
	}
}

// Scenario: Producer ID is unique
func TestProduce_UniqueID(t *testing.T) {
	testutil.Setup(t)

	prod1 := redissmq.NewProducer()
	prod2 := redissmq.NewProducer()

	if prod1.ID() == prod2.ID() {
		t.Fatal("producer IDs should be unique")
	}
	if prod1.ID() == "" {
		t.Fatal("producer ID should not be empty")
	}
}

// Scenario: Producer run is idempotent
func TestProduce_RunTwice(t *testing.T) {
	ctx := testutil.Setup(t)

	prod := redissmq.NewProducer()

	err := prod.Run(ctx)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	err = prod.Run(ctx)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	prod.Shutdown(ctx)
}

// Scenario: Shutdown is idempotent
func TestProduce_ShutdownIdempotent(t *testing.T) {
	ctx := testutil.Setup(t)

	prod := redissmq.NewProducer()
	prod.Run(ctx)

	prod.Shutdown(ctx)
	prod.Shutdown(ctx)
	prod.Shutdown(ctx)
}

// Scenario: A clean Run → Shutdown cycle fires the full four-event
// lifecycle sequence in order: goingUp, up, goingDown, down.
//
// Verifies the "PublishGoingDown on Load failure" fix didn't disturb the
// success path — both goingUp/goingDown and up/down are still published
// exactly once each, in the correct order, and both are visible by the
// time Shutdown returns.
//
// Pre-fix, the goingDown/down events were published from Shutdown's
// synchronous cleanup (under prod.mu) — same observable behaviour for
// this test. Post-PR-3, they're published from whichever goroutine wins
// the cleanup race (usually Shutdown's, since it runs inline), and
// Shutdown waits on the watcher's `done` channel before returning — so
// the events are still guaranteed to be delivered by assertion time.
func TestProduce_RunPublishesGoingUpAndGoingDown(t *testing.T) {
	ctx := testutil.Setup(t)

	var mu sync.Mutex
	var events []string

	subGoingUp, err := publicproducer.SubscribeGoingUp(func(p publicproducer.LifecyclePayload) {
		mu.Lock()
		events = append(events, "goingUp")
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe goingUp: %v", err)
	}
	defer subGoingUp.Unsubscribe()

	subUp, err := publicproducer.SubscribeUp(func(p publicproducer.LifecyclePayload) {
		mu.Lock()
		events = append(events, "up")
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe up: %v", err)
	}
	defer subUp.Unsubscribe()

	subGoingDown, err := publicproducer.SubscribeGoingDown(func(p publicproducer.LifecyclePayload) {
		mu.Lock()
		events = append(events, "goingDown")
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe goingDown: %v", err)
	}
	defer subGoingDown.Unsubscribe()

	subDown, err := publicproducer.SubscribeDown(func(p publicproducer.LifecyclePayload) {
		mu.Lock()
		events = append(events, "down")
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe down: %v", err)
	}
	defer subDown.Unsubscribe()

	prod := redissmq.NewProducer()
	if err := prod.Run(ctx); err != nil {
		t.Fatalf("run producer: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	prod.Shutdown(ctx)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	got := append([]string(nil), events...)
	mu.Unlock()

	want := []string{"goingUp", "up", "goingDown", "down"}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("events[%d] = %q, want %q (full sequence: %v)", i, got[i], want[i], got)
		}
	}
}

// Scenario: No goroutine leak across repeated producer Run → Shutdown
// cycles when the outer ctx is never cancelled.
func TestProduce_ShutdownNoWatcherLeak(t *testing.T) {
	ctx := testutil.Setup(t)

	// Warm-up cycle.
	prod0 := redissmq.NewProducer()
	if err := prod0.Run(ctx); err != nil {
		t.Fatalf("warm-up run: %v", err)
	}
	prod0.Shutdown(ctx)
	time.Sleep(100 * time.Millisecond)

	baseline := runtime.NumGoroutine()

	const cycles = 10
	for i := 0; i < cycles; i++ {
		prod := redissmq.NewProducer()
		if err := prod.Run(ctx); err != nil {
			t.Fatalf("cycle %d run: %v", i, err)
		}
		prod.Shutdown(ctx)
	}

	// Give the runtime a moment to reap any exiting goroutines.
	time.Sleep(300 * time.Millisecond)

	after := runtime.NumGoroutine()
	delta := after - baseline

	// Slack of 3 for scheduler noise, testutil housekeeping, and one-off
	// background goroutines unrelated to producers. Pre-fix, each cycle
	// leaks one watcher — delta would be ~+10, well above the threshold.
	if delta > 3 {
		t.Fatalf("goroutine leak across %d producer Run/Shutdown cycles: "+
			"baseline=%d after=%d delta=+%d — watcher goroutines are leaking",
			cycles, baseline, after, delta)
	}
	t.Logf("goroutine delta after %d cycles: %+d (baseline %d)", cycles, delta, baseline)
}

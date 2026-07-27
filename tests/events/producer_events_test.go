/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package events_test

import (
	"sync"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/producer/events"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	producerEvents "github.com/weyoss/go-redis-smq/pkg/producer/events"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Subscribe to producer lifecycle events
func TestProducerEvents_Lifecycle(t *testing.T) {
	ctx := testutil.Setup(t)

	var mu sync.Mutex
	var lifecycle []string

	subUp, _ := producerEvents.SubscribeUp(func(p events.LifecyclePayload) {
		mu.Lock()
		lifecycle = append(lifecycle, "up:"+p.ProducerID)
		mu.Unlock()
	})
	defer subUp.Unsubscribe()

	subDown, _ := producerEvents.SubscribeDown(func(p events.LifecyclePayload) {
		mu.Lock()
		lifecycle = append(lifecycle, "down:"+p.ProducerID)
		mu.Unlock()
	})
	defer subDown.Unsubscribe()

	subGoingUp, _ := producerEvents.SubscribeGoingUp(func(p events.LifecyclePayload) {
		mu.Lock()
		lifecycle = append(lifecycle, "goingUp:"+p.ProducerID)
		mu.Unlock()
	})
	defer subGoingUp.Unsubscribe()

	subGoingDown, _ := producerEvents.SubscribeGoingDown(func(p events.LifecyclePayload) {
		mu.Lock()
		lifecycle = append(lifecycle, "goingDown:"+p.ProducerID)
		mu.Unlock()
	})
	defer subGoingDown.Unsubscribe()

	// Create and run producer
	prod := redissmq.NewProducer()
	if err := prod.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	prod.Shutdown(ctx)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	events := lifecycle
	mu.Unlock()

	if len(events) != 4 {
		t.Fatalf("expected 4 lifecycle events, got %d: %v", len(events), events)
	}

	foundGoingUp := false
	foundUp := false
	foundGoingDown := false
	foundDown := false
	for _, e := range events {
		switch {
		case len(e) >= 8 && e[:8] == "goingUp:":
			foundGoingUp = true
		case len(e) >= 3 && e[:3] == "up:":
			foundUp = true
		case len(e) >= 10 && e[:10] == "goingDown:":
			foundGoingDown = true
		case len(e) >= 5 && e[:5] == "down:":
			foundDown = true
		}
	}

	if !foundGoingUp {
		t.Error("missing goingUp event")
	}
	if !foundUp {
		t.Error("missing up event")
	}
	if !foundGoingDown {
		t.Error("missing goingDown event")
	}
	if !foundDown {
		t.Error("missing down event")
	}
}

// Scenario: Subscribe to message published event — direct to queue
func TestProducerEvents_MessagePublished_DirectToQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-msg-pub-direct")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received events.MessagePublishedPayload
	sub, err := producerEvents.SubscribeMessagePublished(func(p events.MessagePublishedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("hello").SetQueue(params)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.MessageID != ids[0] {
		t.Errorf("messageID = %s, want %s", received.MessageID, ids[0])
	}
	if received.Queue.Name() != params.Name() {
		t.Errorf("queue = %s, want %s", received.Queue.Name(), params.Name())
	}
	if received.ProducerID == "" {
		t.Error("producerID should not be empty")
	}
}

// Scenario: Subscribe to message published event — scheduled message
func TestProducerEvents_MessagePublished_Scheduled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-msg-pub-sched")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var mu sync.Mutex
	var publishedIDs []string

	sub, _ := producerEvents.SubscribeMessagePublished(func(p events.MessagePublishedPayload) {
		mu.Lock()
		publishedIDs = append(publishedIDs, p.MessageID)
		mu.Unlock()
	})
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)

	// Immediate message — should trigger event
	m1 := msg.New().SetBody("immediate").SetQueue(params)
	ids1, _ := prod.Produce(ctx, m1)

	// Scheduled message — should NOT trigger event (scheduled, not pending)
	m2 := msg.New().SetBody("scheduled").SetQueue(params).SetScheduledDelay(1 * time.Hour)
	prod.Produce(ctx, m2)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	count := len(publishedIDs)
	mu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 published event (immediate only), got %d", count)
	}
	if publishedIDs[0] != ids1[0] {
		t.Errorf("published ID = %s, want %s", publishedIDs[0], ids1[0])
	}
}

// Scenario: Message published event includes correct producer ID
func TestProducerEvents_MessagePublished_ProducerID(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-msg-pub-prodid")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received events.MessagePublishedPayload
	sub, _ := producerEvents.SubscribeMessagePublished(func(p events.MessagePublishedPayload) {
		received = p
		wg.Done()
	})
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	producerID := prod.ID()

	m := msg.New().SetBody("msg").SetQueue(params)
	prod.Produce(ctx, m)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.ProducerID != producerID {
		t.Errorf("producerID = %s, want %s", received.ProducerID, producerID)
	}
}

// Scenario: Multiple subscribers for message published event
func TestProducerEvents_MultipleSubscribers(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-msg-pub-multi")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(3)

	var mu sync.Mutex
	var received []events.MessagePublishedPayload

	handler := func(p events.MessagePublishedPayload) {
		mu.Lock()
		received = append(received, p)
		mu.Unlock()
		wg.Done()
	}

	sub1, _ := producerEvents.SubscribeMessagePublished(handler)
	sub2, _ := producerEvents.SubscribeMessagePublished(handler)
	sub3, _ := producerEvents.SubscribeMessagePublished(handler)
	defer sub1.Unsubscribe()
	defer sub2.Unsubscribe()
	defer sub3.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	m := msg.New().SetBody("msg").SetQueue(params)
	prod.Produce(ctx, m)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for events")
	}

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 3 {
		t.Fatalf("expected 3 events, got %d", count)
	}
}

// Scenario: Producer events across multiple producers
func TestProducerEvents_MultipleProducers(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-multi-prod")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var mu sync.Mutex
	producerIDs := make(map[string]bool)

	sub, _ := producerEvents.SubscribeUp(func(p events.LifecyclePayload) {
		mu.Lock()
		producerIDs[p.ProducerID] = true
		mu.Unlock()
	})
	defer sub.Unsubscribe()

	prod1 := testutil.StartProducer(t, ctx)
	prod2 := testutil.StartProducer(t, ctx)
	prod3 := testutil.StartProducer(t, ctx)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	count := len(producerIDs)
	mu.Unlock()

	if count < 3 {
		t.Fatalf("expected at least 3 producer IDs, got %d", count)
	}

	prod1.Shutdown(ctx)
	prod2.Shutdown(ctx)
	prod3.Shutdown(ctx)
}

// Scenario: Unsubscribe stops receiving producer events
func TestProducerEvents_Unsubscribe(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-prod-unsub")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var mu sync.Mutex
	var msgCount int

	sub, _ := producerEvents.SubscribeMessagePublished(func(p events.MessagePublishedPayload) {
		mu.Lock()
		msgCount++
		mu.Unlock()
	})

	prod := testutil.StartProducer(t, ctx)

	// First message — should trigger event
	prod.Produce(ctx, msg.New().SetBody("msg1").SetQueue(params))
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	firstCount := msgCount
	mu.Unlock()

	if firstCount != 1 {
		t.Fatalf("expected 1 event before unsubscribe, got %d", firstCount)
	}

	// Unsubscribe
	sub.Unsubscribe()

	// Second message — should NOT trigger event
	prod.Produce(ctx, msg.New().SetBody("msg2").SetQueue(params))
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	secondCount := msgCount
	mu.Unlock()

	if secondCount != 1 {
		t.Fatalf("expected still 1 event after unsubscribe, got %d", secondCount)
	}
}

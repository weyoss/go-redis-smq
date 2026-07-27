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

	"github.com/weyoss/go-redis-smq/internal/queue/events"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	queueEvents "github.com/weyoss/go-redis-smq/pkg/queue/events"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Subscribe to queue created event
func TestQueueEvents_SubscribeCreated(t *testing.T) {
	ctx := testutil.Setup(t)

	var wg sync.WaitGroup
	wg.Add(1)

	var received events.CreatedPayload
	sub, err := queueEvents.SubscribeCreated(func(p events.CreatedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	params := q.MustQueueParams("test-events-created")
	if err := queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint); err != nil {
		t.Fatalf("create: %v", err)
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

	if received.Queue.Name() != "test-events-created" {
		t.Errorf("queue name = %s, want test-events-created", received.Queue.Name())
	}
	if received.Properties.Type != q.TypeFIFO {
		t.Errorf("type = %v, want FIFO", received.Properties.Type)
	}
}

// Scenario: Subscribe to queue deleted event
func TestQueueEvents_SubscribeDeleted(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-deleted")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received events.DeletedPayload
	sub, err := queueEvents.SubscribeDeleted(func(p events.DeletedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := queue.Delete(ctx, params); err != nil {
		t.Fatalf("delete: %v", err)
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

	if received.Queue.Name() != "test-events-deleted" {
		t.Errorf("queue name = %s, want test-events-deleted", received.Queue.Name())
	}
}

// Scenario: Subscribe to state changed event
func TestQueueEvents_SubscribeStateChanged(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-state")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var mu sync.Mutex
	var transitions []events.StateChangedPayload

	sub, err := queueEvents.SubscribeStateChanged(func(p events.StateChangedPayload) {
		mu.Lock()
		transitions = append(transitions, p)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Pause
	queue.Pause(ctx, params, nil)
	time.Sleep(200 * time.Millisecond)

	// Resume
	queue.Resume(ctx, params, nil)
	time.Sleep(200 * time.Millisecond)

	// Stop
	queue.Stop(ctx, params, nil)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	count := len(transitions)
	mu.Unlock()

	if count < 3 {
		t.Fatalf("expected at least 3 state changes, got %d", count)
	}

	// Verify the transitions include the expected states
	foundPaused := false
	foundActive := false
	foundStopped := false
	for _, tr := range transitions {
		switch tr.Transition.To {
		case q.StatePaused:
			foundPaused = true
		case q.StateActive:
			foundActive = true
		case q.StateStopped:
			foundStopped = true
		}
	}

	if !foundPaused {
		t.Error("missing PAUSED transition")
	}
	if !foundActive {
		t.Error("missing ACTIVE transition")
	}
	if !foundStopped {
		t.Error("missing STOPPED transition")
	}
}

// Scenario: Subscribe to consumer group created event
func TestQueueEvents_SubscribeConsumerGroupCreated(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-cg-created")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPubSub)

	var wg sync.WaitGroup
	wg.Add(1)

	var received events.ConsumerGroupCreatedPayload
	sub, err := queueEvents.SubscribeConsumerGroupCreated(func(p events.ConsumerGroupCreatedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	result, err := queue.SaveConsumerGroup(ctx, params, "email-service")
	if err != nil {
		t.Fatalf("save group: %v", err)
	}
	if result != 1 {
		t.Fatalf("expected new group, got result=%d", result)
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

	if received.GroupID != "email-service" {
		t.Errorf("groupID = %s, want email-service", received.GroupID)
	}
	if received.Queue.Name() != "test-events-cg-created" {
		t.Errorf("queue name = %s", received.Queue.Name())
	}
}

// Scenario: Subscribe to consumer group deleted event
func TestQueueEvents_SubscribeConsumerGroupDeleted(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-cg-deleted")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPubSub)

	queue.SaveConsumerGroup(ctx, params, "sms-service")

	var wg sync.WaitGroup
	wg.Add(1)

	var received events.ConsumerGroupDeletedPayload
	sub, err := queueEvents.SubscribeConsumerGroupDeleted(func(p events.ConsumerGroupDeletedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := queue.DeleteConsumerGroup(ctx, params, "sms-service"); err != nil {
		t.Fatalf("delete group: %v", err)
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

	if received.GroupID != "sms-service" {
		t.Errorf("groupID = %s, want sms-service", received.GroupID)
	}
	if received.Queue.Name() != "test-events-cg-deleted" {
		t.Errorf("queue name = %s", received.Queue.Name())
	}
}

// Scenario: Multiple subscribers for same event
func TestQueueEvents_MultipleSubscribers(t *testing.T) {
	ctx := testutil.Setup(t)

	var wg sync.WaitGroup
	wg.Add(3)

	var mu sync.Mutex
	var received []events.CreatedPayload

	handler := func(p events.CreatedPayload) {
		mu.Lock()
		received = append(received, p)
		mu.Unlock()
		wg.Done()
	}

	sub1, _ := queueEvents.SubscribeCreated(handler)
	sub2, _ := queueEvents.SubscribeCreated(handler)
	sub3, _ := queueEvents.SubscribeCreated(handler)
	defer sub1.Unsubscribe()
	defer sub2.Unsubscribe()
	defer sub3.Unsubscribe()

	params := q.MustQueueParams("test-events-multi-sub")
	queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

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

// Scenario: Unsubscribe stops receiving events
func TestQueueEvents_Unsubscribe(t *testing.T) {
	ctx := testutil.Setup(t)

	var mu sync.Mutex
	var count int

	sub, _ := queueEvents.SubscribeCreated(func(p events.CreatedPayload) {
		mu.Lock()
		count++
		mu.Unlock()
	})

	// Create first queue — should receive event
	params1 := q.MustQueueParams("test-events-unsub-1")
	queue.Create(ctx, params1, q.TypeFIFO, q.DeliveryPointToPoint)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	firstCount := count
	mu.Unlock()

	if firstCount != 1 {
		t.Fatalf("expected 1 event before unsubscribe, got %d", firstCount)
	}

	// Unsubscribe
	sub.Unsubscribe()

	// Create second queue — should NOT receive event
	params2 := q.MustQueueParams("test-events-unsub-2")
	queue.Create(ctx, params2, q.TypeFIFO, q.DeliveryPointToPoint)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	secondCount := count
	mu.Unlock()

	if secondCount != 1 {
		t.Fatalf("expected still 1 event after unsubscribe, got %d", secondCount)
	}
}

// Scenario: Events include correct queue properties
func TestQueueEvents_PropertiesInEvent(t *testing.T) {
	ctx := testutil.Setup(t)

	var wg sync.WaitGroup
	wg.Add(1)

	var received events.CreatedPayload
	sub, _ := queueEvents.SubscribeCreated(func(p events.CreatedPayload) {
		received = p
		wg.Done()
	})
	defer sub.Unsubscribe()

	params := q.MustQueueParams("test-events-props")
	queue.Create(ctx, params, q.TypeLIFO, q.DeliveryPubSub)

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

	if received.Properties.Type != q.TypeLIFO {
		t.Errorf("type = %v, want LIFO", received.Properties.Type)
	}
	if received.Properties.DeliveryModel != q.DeliveryPubSub {
		t.Errorf("deliveryModel = %v, want PubSub", received.Properties.DeliveryModel)
	}
}

// Scenario: State changed event includes transition details
func TestQueueEvents_StateChangedTransitionDetails(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-transition")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received events.StateChangedPayload
	sub, _ := queueEvents.SubscribeStateChanged(func(p events.StateChangedPayload) {
		// Capture only the pause transition
		if p.Transition.To == q.StatePaused {
			received = p
			wg.Done()
		}
	})
	defer sub.Unsubscribe()

	queue.Pause(ctx, params, &q.StateTransitionOptions{
		Reason:      ptr(q.ReasonTesting),
		Description: ptr("Testing state change events"),
	})

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

	if received.Transition.To != q.StatePaused {
		t.Errorf("to = %v, want PAUSED", received.Transition.To)
	}
	if received.Queue.Name() != "test-events-transition" {
		t.Errorf("queue name = %s", received.Queue.Name())
	}
}

func ptr[T any](v T) *T { return &v }

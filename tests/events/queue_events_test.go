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

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Subscribe to queue created event
func TestQueueEvents_SubscribeCreated(t *testing.T) {
	ctx := testutil.Setup(t)

	var wg sync.WaitGroup
	wg.Add(1)

	var received queue.CreatedPayload
	sub, err := queue.SubscribeCreated(func(p queue.CreatedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	params := publicqueue.MustQueueParams("test-events-created")
	if err := redissmq.NewQueueManager().Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint); err != nil {
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
	if received.Properties.Type != publicqueue.TypeFIFO {
		t.Errorf("type = %v, want FIFO", received.Properties.Type)
	}
}

// Scenario: Subscribe to queue deleted event
func TestQueueEvents_SubscribeDeleted(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-events-deleted")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received queue.DeletedPayload
	sub, err := queue.SubscribeDeleted(func(p queue.DeletedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := redissmq.NewQueueManager().Delete(ctx, params); err != nil {
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

	params := publicqueue.MustQueueParams("test-events-state")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	var mu sync.Mutex
	var transitions []queue.StateChangedPayload

	sub, err := queue.SubscribeStateChanged(func(p queue.StateChangedPayload) {
		mu.Lock()
		transitions = append(transitions, p)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	sm := redissmq.NewStateManager()

	// Pause
	sm.Pause(ctx, params, nil)
	time.Sleep(200 * time.Millisecond)

	// Resume
	sm.Resume(ctx, params, nil)
	time.Sleep(200 * time.Millisecond)

	// Stop
	sm.Stop(ctx, params, nil)
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
		case publicqueue.StatePaused:
			foundPaused = true
		case publicqueue.StateActive:
			foundActive = true
		case publicqueue.StateStopped:
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

	params := publicqueue.MustQueueParams("test-events-cg-created")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	var wg sync.WaitGroup
	wg.Add(1)

	var received queue.ConsumerGroupCreatedPayload
	sub, err := queue.SubscribeConsumerGroupCreated(func(p queue.ConsumerGroupCreatedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	result, err := redissmq.NewConsumerGroupManager().Save(ctx, params, "email-service")
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

	params := publicqueue.MustQueueParams("test-events-cg-deleted")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	cgm.Save(ctx, params, "sms-service")

	var wg sync.WaitGroup
	wg.Add(1)

	var received queue.ConsumerGroupDeletedPayload
	sub, err := queue.SubscribeConsumerGroupDeleted(func(p queue.ConsumerGroupDeletedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := cgm.Delete(ctx, params, "sms-service"); err != nil {
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
	var received []queue.CreatedPayload

	handler := func(p queue.CreatedPayload) {
		mu.Lock()
		received = append(received, p)
		mu.Unlock()
		wg.Done()
	}

	sub1, _ := queue.SubscribeCreated(handler)
	sub2, _ := queue.SubscribeCreated(handler)
	sub3, _ := queue.SubscribeCreated(handler)
	defer sub1.Unsubscribe()
	defer sub2.Unsubscribe()
	defer sub3.Unsubscribe()

	params := publicqueue.MustQueueParams("test-events-multi-sub")
	redissmq.NewQueueManager().Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

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

	sub, _ := queue.SubscribeCreated(func(p queue.CreatedPayload) {
		mu.Lock()
		count++
		mu.Unlock()
	})

	qm := redissmq.NewQueueManager()

	// Create first queue — should receive event
	params1 := publicqueue.MustQueueParams("test-events-unsub-1")
	qm.Create(ctx, params1, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
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
	params2 := publicqueue.MustQueueParams("test-events-unsub-2")
	qm.Create(ctx, params2, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
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

	var received queue.CreatedPayload
	sub, _ := queue.SubscribeCreated(func(p queue.CreatedPayload) {
		received = p
		wg.Done()
	})
	defer sub.Unsubscribe()

	qm := redissmq.NewQueueManager()

	params := publicqueue.MustQueueParams("test-events-props")
	qm.Create(ctx, params, publicqueue.TypeLIFO, publicqueue.DeliveryPubSub)

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

	if received.Properties.Type != publicqueue.TypeLIFO {
		t.Errorf("type = %v, want LIFO", received.Properties.Type)
	}
	if received.Properties.DeliveryModel != publicqueue.DeliveryPubSub {
		t.Errorf("deliveryModel = %v, want PubSub", received.Properties.DeliveryModel)
	}
}

// Scenario: State changed event includes transition details
func TestQueueEvents_StateChangedTransitionDetails(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-events-transition")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received queue.StateChangedPayload
	sub, _ := queue.SubscribeStateChanged(func(p queue.StateChangedPayload) {
		// Capture only the pause transition
		if p.Transition.To == publicqueue.StatePaused {
			received = p
			wg.Done()
		}
	})
	defer sub.Unsubscribe()

	sm := redissmq.NewStateManager()

	sm.Pause(ctx, params, &publicqueue.StateTransitionOptions{
		Reason:      ptr(publicqueue.ReasonTesting),
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

	if received.Transition.To != publicqueue.StatePaused {
		t.Errorf("to = %v, want PAUSED", received.Transition.To)
	}
	if received.Queue.Name() != "test-events-transition" {
		t.Errorf("queue name = %s", received.Queue.Name())
	}
}

func ptr[T any](v T) *T { return &v }

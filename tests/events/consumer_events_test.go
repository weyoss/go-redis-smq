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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/consumer"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicConsumer "github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Subscribe to consumer lifecycle events
func TestConsumerEvents_Lifecycle(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-cons-lifecycle")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var mu sync.Mutex
	var lifecycle []string

	subUp, _ := publicConsumer.SubscribeUp(func(p publicConsumer.LifecyclePayload) {
		mu.Lock()
		lifecycle = append(lifecycle, "up:"+p.ConsumerID)
		mu.Unlock()
	})
	defer subUp.Unsubscribe()

	subDown, _ := publicConsumer.SubscribeDown(func(p publicConsumer.LifecyclePayload) {
		mu.Lock()
		lifecycle = append(lifecycle, "down:"+p.ConsumerID)
		mu.Unlock()
	})
	defer subDown.Unsubscribe()

	subGoingUp, _ := publicConsumer.SubscribeGoingUp(func(p publicConsumer.LifecyclePayload) {
		mu.Lock()
		lifecycle = append(lifecycle, "goingUp:"+p.ConsumerID)
		mu.Unlock()
	})
	defer subGoingUp.Unsubscribe()

	subGoingDown, _ := publicConsumer.SubscribeGoingDown(func(p publicConsumer.LifecyclePayload) {
		mu.Lock()
		lifecycle = append(lifecycle, "goingDown:"+p.ConsumerID)
		mu.Unlock()
	})
	defer subGoingDown.Unsubscribe()

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	cons.Shutdown()
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	events := lifecycle
	mu.Unlock()

	if len(events) != 4 {
		t.Fatalf("expected 4 lifecycle events, got %d: %v", len(events), events)
	}
}

// Scenario: Subscribe to message acknowledged event
func TestConsumerEvents_MessageAcknowledged(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-ack")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received publicConsumer.MessagePayload
	sub, _ := publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
		received = p
		wg.Done()
	})
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().SetBody("ack-me").SetQueue(params))

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.MessageID != ids[0] {
		t.Errorf("messageID = %s, want %s", received.MessageID, ids[0])
	}
	if received.Queue.Name() != params.Name() {
		t.Errorf("queue = %s, want %s", received.Queue.Name(), params.Name())
	}
	if received.ConsumerID == "" {
		t.Error("consumerID should not be empty")
	}
}

// Scenario: Subscribe to message unacknowledged event
func TestConsumerEvents_MessageUnacknowledged(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-unack")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received publicConsumer.MessageUnacknowledgedPayload
	sub, _ := publicConsumer.SubscribeMessageUnacknowledged(func(p publicConsumer.MessageUnacknowledgedPayload) {
		received = p
		wg.Done()
	})
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("fail-me").SetQueue(params).SetRetryThreshold(3).SetRetryDelay(0))

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("handler error")
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.Cause != int(consumer.CauseUnacknowledged) {
		t.Errorf("cause = %d, want %d", received.Cause, consumer.CauseUnacknowledged)
	}
	if received.ConsumerID == "" {
		t.Error("consumerID should not be empty")
	}
}

// Scenario: Subscribe to message dead-lettered event
func TestConsumerEvents_MessageDeadLettered(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-events-dlq")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received publicConsumer.MessageDeadLetteredPayload
	sub, _ := publicConsumer.SubscribeMessageDeadLettered(func(p publicConsumer.MessageDeadLetteredPayload) {
		received = p
		wg.Done()
	})
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("dlq-me").SetQueue(params).SetRetryThreshold(1).SetRetryDelay(0))

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("always fail")
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.Cause != int(consumer.DeadLetterRetryThresholdExceeded) {
		t.Errorf("cause = %d, want %d", received.Cause, consumer.DeadLetterRetryThresholdExceeded)
	}
	if received.Queue.Name() != params.Name() {
		t.Errorf("queue = %s, want %s", received.Queue.Name(), params.Name())
	}
}

// Scenario: Subscribe to message requeued event
func TestConsumerEvents_MessageRequeued(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-requeue")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received publicConsumer.MessagePayload
	sub, _ := publicConsumer.SubscribeMessageRequeued(func(p publicConsumer.MessagePayload) {
		received = p
		wg.Done()
	})
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("requeue-me").SetQueue(params).SetRetryThreshold(3).SetRetryDelay(0))

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("fail")
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.Queue.Name() != params.Name() {
		t.Errorf("queue = %s, want %s", received.Queue.Name(), params.Name())
	}
}

// Scenario: Subscribe to message delayed event
func TestConsumerEvents_MessageDelayed(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-delayed")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received publicConsumer.MessagePayload
	sub, _ := publicConsumer.SubscribeMessageDelayed(func(p publicConsumer.MessagePayload) {
		received = p
		wg.Done()
	})
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("delay-me").SetQueue(params).SetRetryThreshold(3).SetRetryDelay(100*time.Millisecond))

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("fail")
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.Queue.Name() != params.Name() {
		t.Errorf("queue = %s, want %s", received.Queue.Name(), params.Name())
	}
}

// Scenario: Multiple subscribers for same consumer event
func TestConsumerEvents_MultipleSubscribers(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-cons-multi")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(3)

	var mu sync.Mutex
	var received []publicConsumer.MessagePayload

	handler := func(p publicConsumer.MessagePayload) {
		mu.Lock()
		received = append(received, p)
		mu.Unlock()
		wg.Done()
	}

	sub1, _ := publicConsumer.SubscribeMessageAcknowledged(handler)
	sub2, _ := publicConsumer.SubscribeMessageAcknowledged(handler)
	sub3, _ := publicConsumer.SubscribeMessageAcknowledged(handler)
	defer sub1.Unsubscribe()
	defer sub2.Unsubscribe()
	defer sub3.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("multi").SetQueue(params))

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for events")
	}

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 3 {
		t.Fatalf("expected 3 events, got %d", count)
	}
}

// Scenario: Unsubscribe stops receiving consumer events
func TestConsumerEvents_Unsubscribe(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-cons-unsub")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var mu sync.Mutex
	var ackCount int

	sub, _ := publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
		mu.Lock()
		ackCount++
		mu.Unlock()
	})

	prod := testutil.StartProducer(t, ctx)

	// First message — should trigger event
	prod.Produce(ctx, msg.New().SetBody("first").SetQueue(params))
	cons1 := redissmq.NewConsumer()
	cons1.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons1.Run(ctx)
	time.Sleep(2 * time.Second)
	cons1.Shutdown()
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	firstCount := ackCount
	mu.Unlock()

	if firstCount != 1 {
		t.Fatalf("expected 1 event before unsubscribe, got %d", firstCount)
	}

	// Unsubscribe
	sub.Unsubscribe()

	// Second message — should NOT trigger event
	prod.Produce(ctx, msg.New().SetBody("second").SetQueue(params))
	cons2 := redissmq.NewConsumer()
	cons2.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons2.Run(ctx)
	time.Sleep(2 * time.Second)
	cons2.Shutdown()
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	secondCount := ackCount
	mu.Unlock()

	if secondCount != 1 {
		t.Fatalf("expected still 1 event after unsubscribe, got %d", secondCount)
	}
}

// Scenario: All consumer message events fire in correct order during normal flow
func TestConsumerEvents_NormalFlow(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-normal-flow")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var mu sync.Mutex
	var eventOrder []string

	publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
		mu.Lock()
		eventOrder = append(eventOrder, "acknowledged")
		mu.Unlock()
	})

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("normal").SetQueue(params))

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(3 * time.Second)

	mu.Lock()
	count := len(eventOrder)
	mu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 acknowledged event, got %d: %v", count, eventOrder)
	}
	if eventOrder[0] != "acknowledged" {
		t.Errorf("expected 'acknowledged', got %s", eventOrder[0])
	}
}

// Scenario: Consumer events include correct consumer and queue info
func TestConsumerEvents_EventPayloadInfo(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-payload-info")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received publicConsumer.MessagePayload
	sub, _ := publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
		received = p
		wg.Done()
	})
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().SetBody("info").SetQueue(params))

	cons := redissmq.NewConsumer()
	consumerID := cons.ID()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.MessageID != ids[0] {
		t.Errorf("messageID = %s, want %s", received.MessageID, ids[0])
	}
	if received.ConsumerID != consumerID {
		t.Errorf("consumerID = %s, want %s", received.ConsumerID, consumerID)
	}
	if received.Queue.Name() != params.Name() {
		t.Errorf("queue = %s, want %s", received.Queue.Name(), params.Name())
	}
}

// Scenario: Subscribe to message received event
func TestConsumerEvents_MessageReceived(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-msg-received")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received publicConsumer.MessageReceivedPayload
	sub, err := publicConsumer.SubscribeMessageReceived(func(p publicConsumer.MessageReceivedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().SetBody("receive-me").SetQueue(params))

	cons := redissmq.NewConsumer()
	consumerID := cons.ID()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.MessageID != ids[0] {
		t.Errorf("messageID = %s, want %s", received.MessageID, ids[0])
	}
	if received.Queue.Name() != params.Name() {
		t.Errorf("queue = %s, want %s", received.Queue.Name(), params.Name())
	}
	if received.ConsumerID != consumerID {
		t.Errorf("consumerID = %s, want %s", received.ConsumerID, consumerID)
	}
}

// Scenario: Message received event fires before acknowledged event
func TestConsumerEvents_MessageReceivedOrdering(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-events-msg-received-ordering")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	var mu sync.Mutex
	var eventOrder []string

	publicConsumer.SubscribeMessageReceived(func(p publicConsumer.MessageReceivedPayload) {
		mu.Lock()
		eventOrder = append(eventOrder, "received")
		mu.Unlock()
	})
	publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
		mu.Lock()
		eventOrder = append(eventOrder, "acknowledged")
		mu.Unlock()
	})

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("order").SetQueue(params))

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(3 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(eventOrder) != 2 {
		t.Fatalf("expected 2 events, got %d: %v", len(eventOrder), eventOrder)
	}
	if eventOrder[0] != "received" {
		t.Errorf("expected 'received' first, got %s", eventOrder[0])
	}
	if eventOrder[1] != "acknowledged" {
		t.Errorf("expected 'acknowledged' second, got %s", eventOrder[1])
	}
}

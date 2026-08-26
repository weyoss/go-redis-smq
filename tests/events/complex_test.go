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
	"github.com/weyoss/go-redis-smq/internal/config"
	internalConfigEvents "github.com/weyoss/go-redis-smq/internal/config/events"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicConsumer "github.com/weyoss/go-redis-smq/pkg/consumer"
	publicEventBus "github.com/weyoss/go-redis-smq/pkg/eventbus"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/producer"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: All event types during normal produce→consume→ack flow
func TestComplexEvents_NormalFlow(t *testing.T) {
	ctx := testutil.Setup(t)

	var mu sync.Mutex
	eventTypes := make(map[string]int)

	sub1, _ := queue.SubscribeCreated(func(p queue.CreatedPayload) {
		mu.Lock()
		eventTypes["queue.created"]++
		mu.Unlock()
	})
	defer sub1.Unsubscribe()

	sub2, _ := producer.SubscribeUp(func(p producer.LifecyclePayload) {
		mu.Lock()
		eventTypes["producer.up"]++
		mu.Unlock()
	})
	defer sub2.Unsubscribe()

	sub3, _ := producer.SubscribeMessagePublished(func(p producer.MessagePublishedPayload) {
		mu.Lock()
		eventTypes["producer.messagePublished"]++
		mu.Unlock()
	})
	defer sub3.Unsubscribe()

	sub4, _ := publicConsumer.SubscribeUp(func(p publicConsumer.LifecyclePayload) {
		mu.Lock()
		eventTypes["consumer.up"]++
		mu.Unlock()
	})
	defer sub4.Unsubscribe()

	sub5, _ := publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
		mu.Lock()
		eventTypes["consumer.acknowledged"]++
		mu.Unlock()
	})
	defer sub5.Unsubscribe()

	params := queue.MustQueueParams("test-complex-normal")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

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
	defer mu.Unlock()

	expected := []string{
		"queue.created",
		"producer.up",
		"producer.messagePublished",
		"consumer.up",
		"consumer.acknowledged",
	}

	for _, e := range expected {
		if eventTypes[e] == 0 {
			t.Errorf("missing event: %s", e)
		}
	}

	t.Logf("events: %v", eventTypes)
}

// Scenario: Events during error flow (retry → dead-letter)
func TestComplexEvents_ErrorFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-complex-error")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	var mu sync.Mutex
	eventTypes := make(map[string]int)

	sub1, _ := publicConsumer.SubscribeMessageUnacknowledged(func(p publicConsumer.MessageUnacknowledgedPayload) {
		mu.Lock()
		eventTypes["consumer.unacknowledged"]++
		mu.Unlock()
	})
	defer sub1.Unsubscribe()

	sub2, _ := publicConsumer.SubscribeMessageRequeued(func(p publicConsumer.MessagePayload) {
		mu.Lock()
		eventTypes["consumer.requeued"]++
		mu.Unlock()
	})
	defer sub2.Unsubscribe()

	sub3, _ := publicConsumer.SubscribeMessageDeadLettered(func(p publicConsumer.MessageDeadLetteredPayload) {
		mu.Lock()
		eventTypes["consumer.deadLettered"]++
		mu.Unlock()
	})
	defer sub3.Unsubscribe()

	sub4, _ := publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
		mu.Lock()
		eventTypes["consumer.acknowledged"]++
		mu.Unlock()
	})
	defer sub4.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)

	prod.Produce(ctx, msg.New().SetBody("fail").SetQueue(params).SetRetryThreshold(1).SetRetryDelay(0))
	prod.Produce(ctx, msg.New().SetBody("success").SetQueue(params))

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		count := attempts.Add(1)
		if count == 1 {
			return fmt.Errorf("fail")
		}
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(10 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if eventTypes["consumer.unacknowledged"] < 1 {
		t.Error("missing unacknowledged event")
	}
	if eventTypes["consumer.deadLettered"] < 1 {
		t.Error("missing dead-lettered event")
	}
	if eventTypes["consumer.acknowledged"] < 1 {
		t.Error("missing acknowledged event")
	}

	t.Logf("events: %v", eventTypes)
}

// Scenario: Events during queue state changes while consuming
func TestComplexEvents_QueueStateChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-complex-state")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	var mu sync.Mutex
	stateChanges := make([]string, 0)

	sub, _ := queue.SubscribeStateChanged(func(p queue.StateChangedPayload) {
		mu.Lock()
		stateChanges = append(stateChanges, p.Transition.To.String())
		mu.Unlock()
	})
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	prod.Produce(ctx, msg.New().SetBody("before-pause").SetQueue(params))
	time.Sleep(2 * time.Second)

	sm := redissmq.NewStateManager()

	sm.Pause(ctx, params, nil)
	time.Sleep(2 * time.Second)

	sm.Resume(ctx, params, nil)
	time.Sleep(2 * time.Second)

	prod.Produce(ctx, msg.New().SetBody("after-pause").SetQueue(params))
	time.Sleep(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(stateChanges) < 2 {
		t.Errorf("expected at least 2 state changes, got %d: %v", len(stateChanges), stateChanges)
	}

	foundPaused := false
	foundActive := false
	for _, s := range stateChanges {
		if s == "paused" {
			foundPaused = true
		}
		if s == "active" {
			foundActive = true
		}
	}
	if !foundPaused {
		t.Error("missing paused state change event")
	}
	if !foundActive {
		t.Error("missing active state change event")
	}

	t.Logf("state changes: %v", stateChanges)
	t.Logf("messages consumed: %d", consumed.Load())
}

// Scenario: Config update event while producing and consuming
func TestComplexEvents_ConfigUpdateDuringProcessing(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-complex-config")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	var mu sync.Mutex
	configUpdated := false
	messageAcknowledged := false

	// Subscribe to configuration updates on the system bus using the internal helper.
	sub1, err := internalConfigEvents.SubscribeUpdated(func(p internalConfigEvents.UpdatedPayload) {
		mu.Lock()
		configUpdated = true
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe to config updated: %v", err)
	}
	defer sub1.Unsubscribe()

	sub2, _ := publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
		mu.Lock()
		messageAcknowledged = true
		mu.Unlock()
	})
	defer sub2.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("config-test").SetQueue(params))

	cfg := config.Get()
	cfg.Logger.Enabled = true
	if _, err := config.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(3 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if !configUpdated {
		t.Error("missing config updated event")
	}
	if !messageAcknowledged {
		t.Error("missing message acknowledged event")
	}
}

// Scenario: Cross-domain event ordering
func TestComplexEvents_CrossDomainOrdering(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-complex-ordering")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	var mu sync.Mutex
	var eventOrder []string

	sub1, _ := producer.SubscribeUp(func(p producer.LifecyclePayload) {
		mu.Lock()
		eventOrder = append(eventOrder, "producer.up")
		mu.Unlock()
	})
	defer sub1.Unsubscribe()

	sub2, _ := producer.SubscribeMessagePublished(func(p producer.MessagePublishedPayload) {
		mu.Lock()
		eventOrder = append(eventOrder, "producer.published")
		mu.Unlock()
	})
	defer sub2.Unsubscribe()

	sub3, _ := publicConsumer.SubscribeUp(func(p publicConsumer.LifecyclePayload) {
		mu.Lock()
		eventOrder = append(eventOrder, "consumer.up")
		mu.Unlock()
	})
	defer sub3.Unsubscribe()

	sub4, _ := publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
		mu.Lock()
		eventOrder = append(eventOrder, "consumer.acknowledged")
		mu.Unlock()
	})
	defer sub4.Unsubscribe()

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

	pubIdx := -1
	ackIdx := -1
	for i, e := range eventOrder {
		if e == "producer.published" && pubIdx == -1 {
			pubIdx = i
		}
		if e == "consumer.acknowledged" && ackIdx == -1 {
			ackIdx = i
		}
	}

	if pubIdx == -1 {
		t.Error("missing producer.published event")
	}
	if ackIdx == -1 {
		t.Error("missing consumer.acknowledged event")
	}
	if pubIdx != -1 && ackIdx != -1 && pubIdx >= ackIdx {
		t.Errorf("producer.published (%d) should come before consumer.acknowledged (%d)", pubIdx, ackIdx)
	}

	t.Logf("event order: %v", eventOrder)
}

// Scenario: Many subscribers across all domains
func TestComplexEvents_ManySubscribers(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-complex-many-subs")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	subscriberCount := 5
	var mu sync.Mutex
	var totalEvents int

	var subscriptions []publicEventBus.Subscription

	for i := 0; i < subscriberCount; i++ {
		sub, _ := publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
			mu.Lock()
			totalEvents++
			mu.Unlock()
		})
		subscriptions = append(subscriptions, sub)
	}
	defer func() {
		for _, sub := range subscriptions {
			sub.Unsubscribe()
		}
	}()

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("many").SetQueue(params))

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(3 * time.Second)

	mu.Lock()
	events := totalEvents
	mu.Unlock()

	if events < subscriberCount {
		t.Fatalf("expected at least %d events, got %d", subscriberCount, events)
	}

	t.Logf("received %d events across %d subscribers", events, subscriberCount)
}

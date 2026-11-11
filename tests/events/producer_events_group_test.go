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
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicProducer "github.com/weyoss/go-redis-smq/pkg/producer"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: For a Pub/Sub queue with N consumer groups, the producer
// dispatches one message per group and fires one producer.messagePublished
// event per dispatch — each carrying the distinct group ID. Pre-fix, the
// group ID was serialized away; TS subscribers reading queue.queueParams.name
// and queue.groupId got undefined and undefined.
func TestProducerEvents_MessagePublished_CarriesConsumerGroupID(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-h2-producer-group-id")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPubSub)

	const groupA = "email-service"
	const groupB = "sms-service"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupA); err != nil {
		t.Fatalf("create group A: %v", err)
	}
	if _, err := cgm.Save(ctx, params, groupB); err != nil {
		t.Fatalf("create group B: %v", err)
	}

	var mu sync.Mutex
	receivedGroupIDs := make(map[string]int)
	var receivedQueueNames []string
	var wg sync.WaitGroup
	wg.Add(2) // one event per group

	sub, err := publicProducer.SubscribeMessagePublished(func(p publicProducer.MessagePublishedPayload) {
		mu.Lock()
		receivedGroupIDs[p.Queue.GroupID]++
		receivedQueueNames = append(receivedQueueNames, p.Queue.QueueParams.Name())
		mu.Unlock()
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	ids, err := prod.Produce(ctx, msg.New().SetBody("h2").SetQueue(params))
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 message IDs (one per group), got %d: %v", len(ids), ids)
	}

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
	defer mu.Unlock()

	if receivedGroupIDs[groupA] != 1 {
		t.Errorf("groupA events = %d, want 1 (H2: groupId not threaded into producer event)",
			receivedGroupIDs[groupA])
	}
	if receivedGroupIDs[groupB] != 1 {
		t.Errorf("groupB events = %d, want 1", receivedGroupIDs[groupB])
	}
	for _, name := range receivedQueueNames {
		if name != params.Name() {
			t.Errorf("queueParams.name = %q, want %q", name, params.Name())
		}
	}
}

// Scenario: For a point-to-point queue, the producer event's GroupID is
// empty — the JSON wire format serializes it as null, matching the
// TypeScript interface's `groupId: string | null`.
func TestProducerEvents_MessagePublished_PointToPointHasEmptyGroupID(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-h2-producer-p2p-group")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	var wg sync.WaitGroup
	wg.Add(1)

	var received publicProducer.MessagePublishedPayload
	sub, err := publicProducer.SubscribeMessagePublished(func(p publicProducer.MessagePublishedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	if _, err := prod.Produce(ctx, msg.New().SetBody("h2-p2p").SetQueue(params)); err != nil {
		t.Fatalf("produce: %v", err)
	}

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

	if received.Queue.GroupID != "" {
		t.Errorf("groupId = %q, want empty for point-to-point queue", received.Queue.GroupID)
	}
	if received.Queue.QueueParams.Name() != params.Name() {
		t.Errorf("queueParams.name = %q, want %q",
			received.Queue.QueueParams.Name(), params.Name())
	}
}

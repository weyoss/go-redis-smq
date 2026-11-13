/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

package events_test

import (
	"context"
	"sync"
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicProducer "github.com/weyoss/go-redis-smq/pkg/producer"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: producer.messagePublished must fire for a scheduled message
// when the scheduled publisher moves it to pending — not at scheduling
// time, and not never. Pre-fix, the producer correctly did not fire at
// schedule time, but the scheduled publisher did not fire either, so the
// event was never emitted for scheduled messages.
func TestProducerEvents_MessagePublished_ScheduledFires(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-producer-events-scheduled-fires")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	var mu sync.Mutex
	publishedIDs := make(map[string]int)
	sub, err := publicProducer.SubscribeMessagePublished(func(p publicProducer.MessagePublishedPayload) {
		mu.Lock()
		publishedIDs[p.MessageID]++
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Scheduled publisher runs under a consumer (it spawns after the
	// worker lock is acquired). Start one.
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	prod := testutil.StartProducer(t, ctx)

	ids, err := prod.Produce(ctx, msg.New().
		SetBody("scheduled-fires").
		SetQueue(params).
		SetScheduledDelay(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	scheduledID := ids[0]

	// The scheduled publisher ticks every 5 seconds. Wait up to 15s
	// (three ticks — generous for slow CI) for the event to fire.
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			mu.Lock()
			count := publishedIDs[scheduledID]
			mu.Unlock()
			t.Fatalf("timeout: producer.messagePublished did not fire for "+
				"scheduled message %s (count=%d); pre-fix, scheduled "+
				"deliveries never emit the event (2.1)",
				scheduledID, count)
		case <-ticker.C:
			mu.Lock()
			count := publishedIDs[scheduledID]
			mu.Unlock()
			if count >= 1 {
				// Event fired. Tolerate count >= 2 (race caveat in
				// scheduled_publisher.go documents that duplicates are
				// possible under contention) — presence is what matters.
				return
			}
		}
	}
}

// Scenario: repeating scheduled messages must fire producer.messagePublished
// with the CLONE's ID (the clone is what moves to pending on the first
// repeat tick), not the parent's ID. Guards against a naive fix that fires
// for the parent.
func TestProducerEvents_MessagePublished_ScheduledRepeatFiresClone(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-producer-events-scheduled-repeat")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	var mu sync.Mutex
	publishedIDs := make(map[string]int)
	sub, err := publicProducer.SubscribeMessagePublished(func(p publicProducer.MessagePublishedPayload) {
		mu.Lock()
		publishedIDs[p.MessageID]++
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	prod := testutil.StartProducer(t, ctx)

	// repeat = 2 so that the first publisher tick sees a repeat still
	// pending (1 < 2) and takes the clone path. See the test's doc
	// comment for why repeat = 1 does not create a clone.
	ids, err := prod.Produce(ctx, msg.New().
		SetBody("scheduled-repeat").
		SetQueue(params).
		SetScheduledRepeat(2).
		SetScheduledRepeatPeriod(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	parentID := ids[0]

	// Wait up to 15s for a published event whose ID is NOT the parent.
	// The parent fires immediately (producer dispatches it), so we need
	// to distinguish the clone event from the parent event.
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			mu.Lock()
			defer mu.Unlock()
			t.Fatalf("timeout: no producer.messagePublished fired for the "+
				"clone of %s (parent count=%d); pre-fix, repeat cycles "+
				"never emit the event (2.1)",
				parentID, publishedIDs[parentID])
		case <-ticker.C:
			mu.Lock()
			var cloneID string
			for id, count := range publishedIDs {
				if id != parentID && count >= 1 {
					cloneID = id
					break
				}
			}
			mu.Unlock()
			if cloneID != "" {
				return
			}
		}
	}
}

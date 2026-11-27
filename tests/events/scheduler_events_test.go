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
	"github.com/weyoss/go-redis-smq/pkg/queue"
	publicScheduler "github.com/weyoss/go-redis-smq/pkg/scheduler"
)

// Scenario: scheduler.messagePublished must fire for a scheduled message
// when the scheduled publisher moves it to pending — not at scheduling
// time, and not never.
//
// Regression guard for the pre-change behavior where the producer
// correctly did not fire producer.messagePublished at schedule time, but
// the scheduled publisher also didn't fire anything — so the transition
// from "scheduled" to "pending" was invisible on the event bus. The
// scheduler.messagePublished event closes that gap.
func TestSchedulerEvents_MessagePublished_ScheduledFires(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-scheduler-published-fires")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	var mu sync.Mutex
	publishedIDs := make(map[string]int)
	sub, err := publicScheduler.SubscribeMessagePublished(func(p publicScheduler.MessagePublishedPayload) {
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
	cons.Consume(params, func(_ context.Context, m *msg.Transferable) error {
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
			t.Fatalf("timeout: scheduler.messagePublished did not fire for "+
				"scheduled message %s (count=%d)",
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

// Scenario: repeating scheduled messages must fire
// scheduler.messagePublished with the CLONE's ID (the clone is what moves
// to pending on the first repeat tick), not the parent's ID. Guards
// against a naive fix that fires for the parent.
func TestSchedulerEvents_MessagePublished_ScheduledRepeatFiresClone(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-scheduler-published-repeat")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	var mu sync.Mutex
	publishedIDs := make(map[string]int)
	sub, err := publicScheduler.SubscribeMessagePublished(func(p publicScheduler.MessagePublishedPayload) {
		mu.Lock()
		publishedIDs[p.MessageID]++
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(_ context.Context, m *msg.Transferable) error {
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
	// The parent never fires scheduler.messagePublished (it stays in the
	// scheduled set); only clones do.
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			mu.Lock()
			defer mu.Unlock()
			t.Fatalf("timeout: no scheduler.messagePublished fired for the "+
				"clone of %s (parent count=%d)",
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

// Scenario: scheduler.messageDeadLettered fires when a scheduled message
// is due but its target consumer group has been deleted.
//
// The message enters the scheduled set while the group exists, then the
// group is deleted before the message becomes due. The scheduled
// publisher discovers the group is gone (publish-scheduled.lua's
// SISMEMBER check) and dead-letters the message, firing the event once
// with the scheduled message's own ID.
//
// Covers the simple-scheduled branch of publish-scheduled.lua (Case 1).
// Also asserts the payload's Queue.GroupID is the deleted group — that
// field is what a monitoring subscriber needs to attribute the dead
// letter to the right group on a Pub/Sub queue.
func TestSchedulerEvents_MessageDeadLettered_GroupDeletedBeforeFire(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-scheduler-dlq-group-gone")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPubSub)

	const groupID = "doomed-group"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("save group: %v", err)
	}

	var mu sync.Mutex
	var deadLetteredEvents []publicScheduler.MessageDeadLetteredPayload
	sub, err := publicScheduler.SubscribeMessageDeadLettered(
		func(p publicScheduler.MessageDeadLetteredPayload) {
			mu.Lock()
			deadLetteredEvents = append(deadLetteredEvents, p)
			mu.Unlock()
		})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Produce the scheduled message BEFORE deleting the group. With only
	// one group in ConsumerGroups at this point, Produce returns exactly
	// one ID — the envelope targeted at groupID.
	prod := testutil.StartProducer(t, ctx)
	ids, err := prod.Produce(ctx, msg.New().
		SetBody("dlq-due-with-group-gone").
		SetQueue(params).
		SetScheduledDelay(2*time.Second),
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID (groupID-targeted), got %d: %v", len(ids), ids)
	}
	scheduledID := ids[0]

	// Delete the group while the message is still scheduled but not yet
	// due. Succeeds because groupID has no active consumers and its
	// pending list is empty (the message is in Scheduled, not Pending).
	if err := cgm.Delete(ctx, params, groupID); err != nil {
		t.Fatalf("delete group: %v", err)
	}

	// Start a consumer so the scheduled publisher runs. Group-less
	// Consume on a Pub/Sub queue creates an ephemeral group — independent
	// of the (now-deleted) groupID, and it doesn't affect the scheduled
	// message's baked-in consumerGroupID field.
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(_ context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			mu.Lock()
			count := len(deadLetteredEvents)
			mu.Unlock()
			t.Fatalf("timeout: scheduler.messageDeadLettered did not fire for %s (count=%d)",
				scheduledID, count)
		case <-ticker.C:
			mu.Lock()
			var event publicScheduler.MessageDeadLetteredPayload
			found := false
			for _, e := range deadLetteredEvents {
				if e.MessageID == scheduledID {
					event = e
					found = true
					break
				}
			}
			mu.Unlock()
			if !found {
				continue
			}

			if event.Queue.QueueParams.Name() != params.Name() {
				t.Errorf("Queue.QueueParams.Name() = %q, want %q",
					event.Queue.QueueParams.Name(), params.Name())
			}
			if event.Queue.GroupID != groupID {
				t.Errorf("Queue.GroupID = %q, want %q "+
					"(should be the deleted group the message was targeted at)",
					event.Queue.GroupID, groupID)
			}
			return
		}
	}
}

// Scenario: same as _GroupDeletedBeforeFire but for a repeating scheduled
// message (publish-scheduled.lua's Case 2, the clone path).
//
// When the group is gone at fire time, the Lua dead-letters the PARENT
// repeating message to stop its cycle, and discards the would-be clone
// for the current firing. The published event's MessageID is therefore
// the parent's ID (what the producer originally returned) — not a
// clone's. dead_lettered_ids contains the parent.
//
// The delay is deliberate: without it, the first repeat-only fire is
// immediate (NextScheduledTimestamp returns now when the repeat count is
// 0 and no CRON is set), racing the group deletion on slow CI. With the
// delay, the parent first fires at produce+2s — plenty of margin for
// the deletion to land, and the event's MessageID is deterministic.
func TestSchedulerEvents_MessageDeadLettered_RepeatGroupGone(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-scheduler-dlq-repeat-group-gone")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPubSub)

	const groupID = "doomed-repeat-group"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("save group: %v", err)
	}

	var mu sync.Mutex
	var deadLetteredEvents []publicScheduler.MessageDeadLetteredPayload
	sub, err := publicScheduler.SubscribeMessageDeadLettered(
		func(p publicScheduler.MessageDeadLetteredPayload) {
			mu.Lock()
			deadLetteredEvents = append(deadLetteredEvents, p)
			mu.Unlock()
		})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	ids, err := prod.Produce(ctx, msg.New().
		SetBody("dlq-repeat-group-gone").
		SetQueue(params).
		SetScheduledDelay(2*time.Second).
		SetScheduledRepeat(2).
		SetScheduledRepeatPeriod(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID (groupID-targeted), got %d: %v", len(ids), ids)
	}
	parentID := ids[0]

	if err := cgm.Delete(ctx, params, groupID); err != nil {
		t.Fatalf("delete group: %v", err)
	}

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(_ context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			mu.Lock()
			count := len(deadLetteredEvents)
			mu.Unlock()
			t.Fatalf("timeout: scheduler.messageDeadLettered did not fire for "+
				"parent %s (count=%d)", parentID, count)
		case <-ticker.C:
			mu.Lock()
			var event publicScheduler.MessageDeadLetteredPayload
			found := false
			for _, e := range deadLetteredEvents {
				if e.MessageID == parentID {
					event = e
					found = true
					break
				}
			}
			mu.Unlock()
			if !found {
				continue
			}

			if event.Queue.QueueParams.Name() != params.Name() {
				t.Errorf("Queue.QueueParams.Name() = %q, want %q",
					event.Queue.QueueParams.Name(), params.Name())
			}
			if event.Queue.GroupID != groupID {
				t.Errorf("Queue.GroupID = %q, want %q "+
					"(the deleted group the parent was targeted at)",
					event.Queue.GroupID, groupID)
			}
			return
		}
	}
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

package producer_test

import (
	"fmt"
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	internalproducer "github.com/weyoss/go-redis-smq/internal/producer"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// waitForCondition polls a condition until it returns true or the timeout
// elapses. Used to wait for event-driven resolver state changes.
func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// Scenario: The resolver drops a queue's entry when its last consumer group
// is deleted. The `isPubSub` flag may or may not remain true depending on
// whether the resolver still tracks the queue — the test asserts the group
// list is empty and the caller would treat it as "Pub/Sub with zero groups"
// if isPubSub is still true. Either outcome is defensible from a caller's
// perspective: the producer pre-flight check re-reads Redis anyway.
func TestProducerResolver_OnConsumerGroupDeleted(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-resolver-group-deleted")
	if err := redissmq.NewQueueManager().Create(ctx, params, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	groupID := "email-service"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("save group: %v", err)
	}

	resolver := internalproducer.NewPubSubTargetResolver("test-producer")
	if err := resolver.Load(ctx); err != nil {
		t.Fatalf("load resolver: %v", err)
	}
	defer resolver.Clear()

	// Wait for initial load to see the group.
	waitForCondition(t, 5*time.Second, func() bool {
		groups, isPubSub := resolver.Resolve(params)
		return isPubSub && len(groups) == 1
	})

	// Delete the consumer group.
	if err := cgm.Delete(ctx, params, groupID); err != nil {
		t.Fatalf("delete group: %v", err)
	}

	// Wait for the resolver to remove the group.
	waitForCondition(t, 5*time.Second, func() bool {
		groups, _ := resolver.Resolve(params)
		return len(groups) == 0
	})
}

// Scenario: The resolver drops a queue's entry when the queue itself is
// deleted. After deletion, isPubSub is false — the queue doesn't exist, so
// "is it Pub/Sub?" is moot from the resolver's perspective.
func TestProducerResolver_OnQueueDeleted(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-resolver-queue-deleted")
	if err := redissmq.NewQueueManager().Create(ctx, params, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	groupID := "sms-service"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("save group: %v", err)
	}

	resolver := internalproducer.NewPubSubTargetResolver("test-producer-queue")
	if err := resolver.Load(ctx); err != nil {
		t.Fatalf("load resolver: %v", err)
	}
	defer resolver.Clear()

	waitForCondition(t, 5*time.Second, func() bool {
		groups, isPubSub := resolver.Resolve(params)
		return isPubSub && len(groups) == 1
	})

	// Delete the queue (empty, no consumers, no bound exchanges).
	if err := redissmq.NewQueueManager().Delete(ctx, params); err != nil {
		t.Fatalf("delete queue: %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		groups, isPubSub := resolver.Resolve(params)
		return len(groups) == 0 && !isPubSub
	})
}

// Scenario: A new consumer group is picked up by the resolver and becomes a
// produce target.
func TestProducerResolver_OnConsumerGroupCreated(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-resolver-group-created")
	if err := redissmq.NewQueueManager().Create(ctx, params, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	resolver := internalproducer.NewPubSubTargetResolver("test-producer-group-created")
	if err := resolver.Load(ctx); err != nil {
		t.Fatalf("load resolver: %v", err)
	}
	defer resolver.Clear()

	// Initially no groups.
	if groups, _ := resolver.Resolve(params); len(groups) != 0 {
		t.Fatalf("expected no groups, got %v", groups)
	}

	// Create a consumer group. This fires consumerGroupCreated.
	groupID := "email-service"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("save group: %v", err)
	}

	// Wait for resolver to pick up the new group.
	waitForCondition(t, 5*time.Second, func() bool {
		groups, isPubSub := resolver.Resolve(params)
		return isPubSub && len(groups) == 1 && groups[0] == groupID
	})
}

// Scenario: A new Pub/Sub queue is added to the resolver's target map with
// zero groups, and a subsequent group creation adds the group to that entry.
func TestProducerResolver_OnQueueCreated(t *testing.T) {
	ctx := testutil.Setup(t)

	resolver := internalproducer.NewPubSubTargetResolver("test-producer-queue-created")
	if err := resolver.Load(ctx); err != nil {
		t.Fatalf("load resolver: %v", err)
	}
	defer resolver.Clear()

	// Initially no targets for any queue.
	if groups, isPubSub := resolver.Resolve(queue.MustQueueParams("nonexistent")); len(groups) != 0 || isPubSub {
		t.Fatalf("expected no targets for nonexistent queue, got %v, isPubSub=%v", groups, isPubSub)
	}

	// Create a Pub/Sub queue. This fires queue.created.
	params := queue.MustQueueParams("test-queue-created-event")
	if err := redissmq.NewQueueManager().Create(ctx, params, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	// Create a group so we can observe the resolver's map has the queue.
	// (Verifying "queue exists with zero groups" directly is covered by a
	// dedicated test below — this test confirms queue+group integration.)
	groupID := "sms-service"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("save group: %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		groups, isPubSub := resolver.Resolve(params)
		return isPubSub && len(groups) == 1 && groups[0] == groupID
	})
}

// Scenario: A point-to-point queue is never added to the resolver's target
// map, so Resolve returns isPubSub=false regardless of any consumer groups
// (P2P queues don't have groups anyway).
//
// This is the caller's signal to publish to queue-level Pending, not to a
// per-group list.
func TestProducerResolver_Resolve_P2PQueueNotPubSub(t *testing.T) {
	ctx := testutil.Setup(t)

	p2pQueue := queue.MustQueueParams("test-resolver-p2p")
	if err := redissmq.NewQueueManager().Create(ctx, p2pQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		t.Fatalf("create p2p queue: %v", err)
	}

	// Marker queue: a Pub/Sub queue with a group whose presence in the
	// resolver proves both queue-created events (this one and the P2P one)
	// have been processed — same bus, same delivery stream. Once the marker
	// is visible, the P2P queue's onQueueCreated handler has definitely run
	// (and no-oped, since P2P queues are filtered out).
	markerQueue := queue.MustQueueParams("test-resolver-marker")
	if err := redissmq.NewQueueManager().Create(ctx, markerQueue, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		t.Fatalf("create marker queue: %v", err)
	}
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, markerQueue, "marker-group"); err != nil {
		t.Fatalf("save marker group: %v", err)
	}

	resolver := internalproducer.NewPubSubTargetResolver("test-producer-p2p")
	if err := resolver.Load(ctx); err != nil {
		t.Fatalf("load resolver: %v", err)
	}
	defer resolver.Clear()

	// Wait for the marker queue to be visible. At this point the resolver
	// has processed every event up to the marker's queueCreated —
	// including the P2P queue's queueCreated.
	waitForCondition(t, 5*time.Second, func() bool {
		groups, isPubSub := resolver.Resolve(markerQueue)
		return isPubSub && len(groups) == 1
	})

	// The P2P queue must NOT be in the resolver's map. Even if onQueueCreated
	// ran for it, the handler filters on DeliveryModel == DeliveryPubSub and
	// does nothing for P2P queues.
	groups, isPubSub := resolver.Resolve(p2pQueue)
	if len(groups) != 0 {
		t.Errorf("P2P queue Resolve returned %d groups, want 0", len(groups))
	}
	if isPubSub {
		t.Errorf("P2P queue Resolve returned isPubSub=true, want false")
	}
}

// Scenario: A Pub/Sub queue with zero consumer groups returns isPubSub=true
// and an empty groups slice.
//
// This is the exact condition the H2 fix keys on: pre-fix, Resolve returned
// only []string, collapsing "not Pub/Sub" and "Pub/Sub with zero groups" into
// the same empty-slice result. Consumers read per-group pending lists, so a
// message published to a Pub/Sub queue without a group would land on the
// queue-level Pending list and never be consumed — silently orphaned.
// resolvePubSubTargets uses this isPubSub=true + empty groups combination to
// emit ErrNoConsumerGroups instead.
func TestProducerResolver_Resolve_PubSubZeroGroupsIsPubSub(t *testing.T) {
	ctx := testutil.Setup(t)

	// Marker queue first, to sync on event delivery below.
	markerQueue := queue.MustQueueParams("test-resolver-zero-marker")
	if err := redissmq.NewQueueManager().Create(ctx, markerQueue, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		t.Fatalf("create marker queue: %v", err)
	}
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, markerQueue, "marker-group"); err != nil {
		t.Fatalf("save marker group: %v", err)
	}

	// The queue under test: Pub/Sub, zero groups.
	emptyQueue := queue.MustQueueParams("test-resolver-zero-groups")
	if err := redissmq.NewQueueManager().Create(ctx, emptyQueue, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		t.Fatalf("create empty pub/sub queue: %v", err)
	}

	resolver := internalproducer.NewPubSubTargetResolver("test-producer-zero")
	if err := resolver.Load(ctx); err != nil {
		t.Fatalf("load resolver: %v", err)
	}
	defer resolver.Clear()

	// Sync on the marker queue being visible.
	waitForCondition(t, 5*time.Second, func() bool {
		groups, isPubSub := resolver.Resolve(markerQueue)
		return isPubSub && len(groups) == 1
	})

	// Now the empty Pub/Sub queue's queueCreated event has been processed.
	// Its entry is []string{} with isPubSub=true — not the same as a
	// non-Pub/Sub queue, which is absent from the map entirely.
	groups, isPubSub := resolver.Resolve(emptyQueue)
	if len(groups) != 0 {
		t.Errorf("empty Pub/Sub queue Resolve returned %d groups, want 0", len(groups))
	}
	if !isPubSub {
		t.Errorf("empty Pub/Sub queue Resolve returned isPubSub=false, want true " +
			"(pre-fix behavior collapsed this to false; the H2 fix depends on it being true)")
	}
}

// Scenario: A queue the resolver has never heard of returns (nil, false).
// The caller treats it as "not Pub/Sub" — the queue-not-found case is handled
// by a subsequent Redis lookup if the produce reaches dispatch.
func TestProducerResolver_Resolve_UnknownQueueNotPubSub(t *testing.T) {
	ctx := testutil.Setup(t)

	resolver := internalproducer.NewPubSubTargetResolver("test-producer-unknown")
	if err := resolver.Load(ctx); err != nil {
		t.Fatalf("load resolver: %v", err)
	}
	defer resolver.Clear()

	unknown := queue.MustQueueParams("test-resolver-never-existed")
	groups, isPubSub := resolver.Resolve(unknown)
	if len(groups) != 0 {
		t.Errorf("unknown queue Resolve returned %d groups, want 0", len(groups))
	}
	if isPubSub {
		t.Errorf("unknown queue Resolve returned isPubSub=true, want false")
	}
}

// Scenario: A Pub/Sub queue created during Load must be visible in the
// resolver once Load returns, even if the queue was created after Load's
// initial AllQueues read.
//
// Regression test for the resolver startup race: pre-fix, Load read
// AllQueues first and subscribed last. A queueCreated event that fired in
// the window between the read and the subscribe was never delivered, so
// the queue stayed invisible until some later event refreshed it. On a
// system with many queues, Load's per-queue loop takes hundreds of
// milliseconds — plenty of time for a concurrent create to land in the
// lost-event window.
//
// Post-fix, subscribe runs first, so the event is caught regardless of
// when during Load the queue was created.
//
// The test is statistical — the pre-fix window is wide here (because of
// the seeded queue count) but not guaranteed to be hit on every run. It
// always passes post-fix.
func TestProducerResolver_LoadRaceWithQueueCreation(t *testing.T) {
	ctx := testutil.Setup(t)

	// Seed enough Pub/Sub queues that Load's loop takes measurable time
	// (2 Redis round-trips per queue × ~1ms = ~600ms for 300 queues),
	// making the pre-fix lost-event window large enough to reliably catch
	// a concurrent create.
	const seedCount = 300
	for i := 0; i < seedCount; i++ {
		qp := queue.MustQueueParams(fmt.Sprintf("test-race-seed-%d", i))
		if err := redissmq.NewQueueManager().Create(ctx, qp, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	targetQueue := queue.MustQueueParams("test-race-target")
	resolver := internalproducer.NewPubSubTargetResolver("test-race")
	defer resolver.Clear()

	loadDone := make(chan error, 1)
	go func() { loadDone <- resolver.Load(ctx) }()

	// Give Load time to complete subscribe (~1ms) and the initial
	// AllQueues read (~1ms), then create the target. Under the pre-fix
	// code, subscribe is at the END of Load — so this create fires long
	// before subscribe, and the queueCreated event is lost.
	time.Sleep(5 * time.Millisecond)

	if err := redissmq.NewQueueManager().Create(ctx, targetQueue, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		t.Fatalf("create target: %v", err)
	}

	if err := <-loadDone; err != nil {
		t.Fatalf("load: %v", err)
	}

	// The target queue must be visible. Post-fix, the queueCreated event
	// was received (subscription was already active) and the handler added
	// the queue. Pre-fix, the event was lost — waitForCondition times out.
	waitForCondition(t, 5*time.Second, func() bool {
		_, isPubSub := resolver.Resolve(targetQueue)
		return isPubSub
	})
}

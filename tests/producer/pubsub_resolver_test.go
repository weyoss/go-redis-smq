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
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	internalproducer "github.com/weyoss/go-redis-smq/internal/producer"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

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
		return len(resolver.Resolve(params)) == 1
	})

	// Delete the consumer group.
	if err := cgm.Delete(ctx, params, groupID); err != nil {
		t.Fatalf("delete group: %v", err)
	}

	// Wait for the resolver to remove the group.
	waitForCondition(t, 5*time.Second, func() bool {
		return len(resolver.Resolve(params)) == 0
	})
}

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
		return len(resolver.Resolve(params)) == 1
	})

	// Delete the queue (empty, no consumers, no bound exchanges).
	if err := redissmq.NewQueueManager().Delete(ctx, params); err != nil {
		t.Fatalf("delete queue: %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		return len(resolver.Resolve(params)) == 0
	})
}

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
	if got := resolver.Resolve(params); len(got) != 0 {
		t.Fatalf("expected no groups, got %v", got)
	}

	// Create a consumer group. This fires consumerGroupCreated.
	groupID := "email-service"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("save group: %v", err)
	}

	// Wait for resolver to pick up the new group.
	waitForCondition(t, 5*time.Second, func() bool {
		groups := resolver.Resolve(params)
		return len(groups) == 1 && groups[0] == groupID
	})
}

func TestProducerResolver_OnQueueCreated(t *testing.T) {
	ctx := testutil.Setup(t)

	resolver := internalproducer.NewPubSubTargetResolver("test-producer-queue-created")
	if err := resolver.Load(ctx); err != nil {
		t.Fatalf("load resolver: %v", err)
	}
	defer resolver.Clear()

	// Initially no targets.
	if got := resolver.Resolve(queue.MustQueueParams("nonexistent")); len(got) != 0 {
		t.Fatalf("expected no targets for nonexistent queue, got %v", got)
	}

	// Create a Pub/Sub queue. This fires queue.created.
	params := queue.MustQueueParams("test-queue-created-event")
	if err := redissmq.NewQueueManager().Create(ctx, params, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	// We can't directly inspect the internal targets map, but we can
	// trigger a follow-up event that depends on the queue being present.
	// For example, creating a consumer group and verifying the resolver
	// sees it implies the queue entry exists (created by onQueueCreated).
	groupID := "sms-service"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("save group: %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		groups := resolver.Resolve(params)
		return len(groups) == 1 && groups[0] == groupID
	})
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue_test

import (
	"testing"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Save consumer group on PubSub queue
func TestConsumerGroup_Save(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cg-save")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	result, err := cgm.Save(ctx, params, "email-service")
	if err != nil {
		t.Fatalf("save consumer group: %v", err)
	}
	if result != 1 {
		t.Fatalf("result = %d, want 1 (created)", result)
	}
}

// Scenario: Save duplicate consumer group returns 0
func TestConsumerGroup_SaveDuplicate(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cg-dup")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	_, err := cgm.Save(ctx, params, "email-service")
	if err != nil {
		t.Fatalf("save consumer group: %v", err)
	}

	result, err := cgm.Save(ctx, params, "email-service")
	if err != nil {
		t.Fatalf("save consumer group: %v", err)
	}
	if result != 0 {
		t.Fatalf("result = %d, want 0 (already exists)", result)
	}
}

// Scenario: Save consumer group on P2P queue returns error
func TestConsumerGroup_SaveOnP2P(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cg-p2p")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	cgm := redissmq.NewConsumerGroupManager()
	_, err := cgm.Save(ctx, params, "email-service")
	if err == nil {
		t.Fatal("expected error for consumer group on P2P queue")
	}
}

// Scenario: List consumer groups
func TestConsumerGroup_List(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cg-list")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	_, err := cgm.Save(ctx, params, "email-service")
	if err != nil {
		t.Fatalf("save email-service: %v", err)
	}
	_, err = cgm.Save(ctx, params, "sms-service")
	if err != nil {
		t.Fatalf("save sms-service: %v", err)
	}

	groups, err := cgm.List(ctx, params)
	if err != nil {
		t.Fatalf("list consumer groups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(groups))
	}

	found := make(map[string]bool)
	for _, g := range groups {
		found[g] = true
	}
	if !found["email-service"] {
		t.Error("missing group: email-service")
	}
	if !found["sms-service"] {
		t.Error("missing group: sms-service")
	}
}

// Scenario: List consumer groups for empty queue
func TestConsumerGroup_ListEmpty(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cg-empty")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	groups, err := cgm.List(ctx, params)
	if err != nil {
		t.Fatalf("list consumer groups: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("groups = %d, want 0", len(groups))
	}
}

// Scenario: Delete consumer group
func TestConsumerGroup_Delete(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cg-delete")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	_, err := cgm.Save(ctx, params, "email-service")
	if err != nil {
		t.Fatalf("save consumer group: %v", err)
	}

	err = cgm.Delete(ctx, params, "email-service")
	if err != nil {
		t.Fatalf("delete consumer group: %v", err)
	}

	groups, _ := cgm.List(ctx, params)
	if len(groups) != 0 {
		t.Fatalf("groups = %d, want 0 after delete", len(groups))
	}
}

// Scenario: Delete non-existent consumer group succeeds (idempotent)
func TestConsumerGroup_DeleteNonExistent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cg-delete-nf")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	err := cgm.Delete(ctx, params, "nonexistent")
	if err != nil {
		t.Fatalf("delete non-existent group should succeed: %v", err)
	}
}

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
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Create a queue and verify it exists
func TestQueue_Create(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-create")

	qm := redissmq.NewQueueManager()

	err := qm.Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	exists, err := qm.Exists(ctx, params)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Fatal("queue should exist after creation")
	}
}

// Scenario: Creating a duplicate queue returns an error
func TestQueue_CreateDuplicate(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-dup")
	qm := redissmq.NewQueueManager()

	err := qm.Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	err = qm.Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	if err == nil {
		t.Fatal("expected duplicate error")
	}
}

// Scenario: Create queues of different types
func TestQueue_CreateDifferentTypes(t *testing.T) {
	ctx := testutil.Setup(t)

	tests := []struct {
		name  string
		qType publicqueue.Type
		model publicqueue.DeliveryModel
	}{
		{"FIFO-P2P", publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint},
		{"LIFO-P2P", publicqueue.TypeLIFO, publicqueue.DeliveryPointToPoint},
		{"Priority-P2P", publicqueue.TypePriority, publicqueue.DeliveryPointToPoint},
		{"FIFO-PubSub", publicqueue.TypeFIFO, publicqueue.DeliveryPubSub},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := publicqueue.MustQueueParams("test-" + tt.name)
			qm := redissmq.NewQueueManager()

			err := qm.Create(ctx, params, tt.qType, tt.model)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			props, err := qm.Properties(ctx, params)
			if err != nil {
				t.Fatalf("properties: %v", err)
			}
			if props.Type != tt.qType {
				t.Errorf("type = %v, want %v", props.Type, tt.qType)
			}
			if props.DeliveryModel != tt.model {
				t.Errorf("delivery model = %v, want %v", props.DeliveryModel, tt.model)
			}
		})
	}
}

// Scenario: Create a queue then set a rate limit
func TestQueue_RateLimit(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-rate-limit")
	qm := redissmq.NewQueueManager()
	err := qm.Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Set rate limit
	rl := publicqueue.MustRateLimitParams(100, time.Minute)
	err = qm.SetRateLimit(ctx, params, rl)
	if err != nil {
		t.Fatalf("set rate limit: %v", err)
	}

	// Verify
	got, err := qm.RateLimit(ctx, params)
	if err != nil {
		t.Fatalf("get rate limit: %v", err)
	}
	if got == nil {
		t.Fatal("expected rate limit")
	}
	if got.Limit() != 100 {
		t.Errorf("limit = %d, want 100", got.Limit())
	}
}

// Scenario: Inspect queue properties
func TestQueue_Properties(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-props")
	qm := redissmq.NewQueueManager()
	err := qm.Create(ctx, params, publicqueue.TypeLIFO, publicqueue.DeliveryPubSub)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	props, err := qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}

	if props.Type != publicqueue.TypeLIFO {
		t.Errorf("type = %v, want LIFO", props.Type)
	}
	if props.DeliveryModel != publicqueue.DeliveryPubSub {
		t.Errorf("delivery model = %v, want PubSub", props.DeliveryModel)
	}
	if props.OperationalState != publicqueue.StateActive {
		t.Errorf("state = %v, want ACTIVE", props.OperationalState)
	}
	if props.MessagesCount != 0 {
		t.Errorf("messages = %d, want 0", props.MessagesCount)
	}
}

// Scenario: Properties of non-existent queue returns error
func TestQueue_PropertiesNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("nonexistent")
	qm := redissmq.NewQueueManager()
	_, err := qm.Properties(ctx, params)
	if err == nil {
		t.Fatal("expected error for non-existent queue")
	}
}

// Scenario: Delete a queue
func TestQueue_Delete(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-delete")
	qm := redissmq.NewQueueManager()
	err := qm.Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	err = qm.Delete(ctx, params)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	exists, err := qm.Exists(ctx, params)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists {
		t.Fatal("queue should not exist after deletion")
	}
}

// Scenario: Delete a non-existent queue returns error
func TestQueue_DeleteNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-delete-notfound")
	err := redissmq.NewQueueManager().Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error for non-existent queue")
	}
}

// Scenario: List all queues
func TestQueue_ListAll(t *testing.T) {
	ctx := testutil.Setup(t)

	qm := redissmq.NewQueueManager()

	// Create a few queues
	names := []string{"list-a", "list-b", "list-c"}
	for _, name := range names {
		params := publicqueue.MustQueueParams(name)
		if err := qm.Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	all, err := qm.ListAll(ctx)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) < 3 {
		t.Fatalf("expected at least 3 queues, got %d", len(all))
	}
}

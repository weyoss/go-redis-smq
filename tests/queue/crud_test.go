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

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Create a queue and verify it exists
func TestQueue_Create(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-create")

	err := queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	exists, err := queue.Exists(ctx, params)
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

	params := q.MustQueueParams("test-dup")

	err := queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	err = queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)
	if err == nil {
		t.Fatal("expected duplicate error")
	}
}

// Scenario: Create queues of different types
func TestQueue_CreateDifferentTypes(t *testing.T) {
	ctx := testutil.Setup(t)

	tests := []struct {
		name  string
		qType q.QueueType
		model q.DeliveryModel
	}{
		{"FIFO-P2P", q.TypeFIFO, q.DeliveryPointToPoint},
		{"LIFO-P2P", q.TypeLIFO, q.DeliveryPointToPoint},
		{"Priority-P2P", q.TypePriority, q.DeliveryPointToPoint},
		{"FIFO-PubSub", q.TypeFIFO, q.DeliveryPubSub},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := q.MustQueueParams("test-" + tt.name)

			err := queue.Create(ctx, params, tt.qType, tt.model)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			props, err := queue.Properties(ctx, params)
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

	params := q.MustQueueParams("test-rate-limit")
	err := queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Set rate limit
	rl := q.MustRateLimitParams(100, time.Minute)
	err = queue.SetRateLimit(ctx, params, rl)
	if err != nil {
		t.Fatalf("set rate limit: %v", err)
	}

	// Verify
	got, err := queue.RateLimit(ctx, params)
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

	params := q.MustQueueParams("test-props")
	err := queue.Create(ctx, params, q.TypeLIFO, q.DeliveryPubSub)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	props, err := queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}

	if props.Type != q.TypeLIFO {
		t.Errorf("type = %v, want LIFO", props.Type)
	}
	if props.DeliveryModel != q.DeliveryPubSub {
		t.Errorf("delivery model = %v, want PubSub", props.DeliveryModel)
	}
	if props.OperationalState != q.StateActive {
		t.Errorf("state = %v, want ACTIVE", props.OperationalState)
	}
	if props.MessagesCount != 0 {
		t.Errorf("messages = %d, want 0", props.MessagesCount)
	}
}

// Scenario: Properties of non-existent queue returns error
func TestQueue_PropertiesNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("nonexistent")
	_, err := queue.Properties(ctx, params)
	if err == nil {
		t.Fatal("expected error for non-existent queue")
	}
}

// Scenario: Delete a queue
func TestQueue_Delete(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delete")
	err := queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	err = queue.Delete(ctx, params)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	exists, err := queue.Exists(ctx, params)
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

	params := q.MustQueueParams("test-delete-notfound")
	err := queue.Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error for non-existent queue")
	}
}

// Scenario: List all queues
func TestQueue_ListAll(t *testing.T) {
	ctx := testutil.Setup(t)

	// Create a few queues
	names := []string{"list-a", "list-b", "list-c"}
	for _, name := range names {
		params := q.MustQueueParams(name)
		if err := queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	all, err := queue.ListAll(ctx)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) < 3 {
		t.Fatalf("expected at least 3 queues, got %d", len(all))
	}
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package namespace_test

import (
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/namespace"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Create queue in namespace
func TestQueue_CreateInNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParamsWithNS("test-ns-queue", "my-ns")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	if params.NS() != "my-ns" {
		t.Errorf("ns = %q, want %q", params.NS(), "my-ns")
	}
}

// Scenario: List queues by namespace
func TestQueue_ListByNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := q.MustQueueParamsWithNS("test-ns-list-q1", "list-ns")
	q2 := q.MustQueueParamsWithNS("test-ns-list-q2", "list-ns")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeFIFO, q.DeliveryPointToPoint)

	queues, err := queue.ListByNamespace(ctx, "list-ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(queues) != 2 {
		t.Fatalf("expected 2 queues, got %d", len(queues))
	}
}

// Scenario: Queues in different namespaces are isolated
func TestQueue_NamespaceIsolation(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := q.MustQueueParamsWithNS("same-name", "ns-a")
	q2 := q.MustQueueParamsWithNS("same-name", "ns-b")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeFIFO, q.DeliveryPointToPoint)

	// Both should exist independently
	exists1, _ := queue.Exists(ctx, q1)
	exists2, _ := queue.Exists(ctx, q2)

	if !exists1 {
		t.Error("queue in ns-a should exist")
	}
	if !exists2 {
		t.Error("queue in ns-b should exist")
	}

	// List by namespace should only return the queue in that namespace
	nsAQueues, _ := queue.ListByNamespace(ctx, "ns-a")
	nsBQueues, _ := queue.ListByNamespace(ctx, "ns-b")

	if len(nsAQueues) != 1 {
		t.Errorf("ns-a should have 1 queue, got %d", len(nsAQueues))
	}
	if len(nsBQueues) != 1 {
		t.Errorf("ns-b should have 1 queue, got %d", len(nsBQueues))
	}
}

// Scenario: Same queue name in different namespaces
func TestQueue_SameNameDifferentNamespaces(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := q.MustQueueParamsWithNS("orders", "production")
	q2 := q.MustQueueParamsWithNS("orders", "staging")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeFIFO, q.DeliveryPointToPoint)

	// Both should exist
	props1, _ := queue.Properties(ctx, q1)
	props2, _ := queue.Properties(ctx, q2)

	if props1 == nil {
		t.Error("production/orders should exist")
	}
	if props2 == nil {
		t.Error("staging/orders should exist")
	}

	// Deleting one should not affect the other
	queue.Delete(ctx, q1)
	exists2, _ := queue.Exists(ctx, q2)
	if !exists2 {
		t.Error("staging/orders should still exist after deleting production/orders")
	}
}

// Scenario: List queues in namespace after deletion
func TestQueue_ListAfterDeletingNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := q.MustQueueParamsWithNS("test-ns-del-q", "temp-queue-ns")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)

	nm := namespace.NewManager()
	nm.Delete(ctx, "temp-queue-ns")

	queues, err := queue.ListByNamespace(ctx, "temp-queue-ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(queues) != 0 {
		t.Errorf("expected 0 queues after namespace delete, got %d", len(queues))
	}
}

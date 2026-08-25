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

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/namespace"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Create queue in namespace
func TestQueue_CreateInNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParamsWithNS("test-ns-queue", "my-ns")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	if params.NS() != "my-ns" {
		t.Errorf("ns = %q, want %q", params.NS(), "my-ns")
	}
}

// Scenario: List queues by namespace
func TestQueue_ListByNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := publicqueue.MustQueueParamsWithNS("test-ns-list-q1", "list-ns")
	q2 := publicqueue.MustQueueParamsWithNS("test-ns-list-q2", "list-ns")
	testutil.CreateQueue(t, ctx, q1, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	queues, err := redissmq.NewQueueManager().ListByNamespace(ctx, "list-ns")
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

	q1 := publicqueue.MustQueueParamsWithNS("same-name", "ns-a")
	q2 := publicqueue.MustQueueParamsWithNS("same-name", "ns-b")
	testutil.CreateQueue(t, ctx, q1, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()

	// Both should exist independently
	exists1, _ := qm.Exists(ctx, q1)
	exists2, _ := qm.Exists(ctx, q2)

	if !exists1 {
		t.Error("queue in ns-a should exist")
	}
	if !exists2 {
		t.Error("queue in ns-b should exist")
	}

	qm = redissmq.NewQueueManager()

	// List by namespace should only return the queue in that namespace
	nsAQueues, _ := qm.ListByNamespace(ctx, "ns-a")
	nsBQueues, _ := qm.ListByNamespace(ctx, "ns-b")

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

	q1 := publicqueue.MustQueueParamsWithNS("orders", "production")
	q2 := publicqueue.MustQueueParamsWithNS("orders", "staging")
	testutil.CreateQueue(t, ctx, q1, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()

	// Both should exist
	props1, _ := qm.Properties(ctx, q1)
	props2, _ := qm.Properties(ctx, q2)

	if props1 == nil {
		t.Error("production/orders should exist")
	}
	if props2 == nil {
		t.Error("staging/orders should exist")
	}

	// Deleting one should not affect the other
	qm.Delete(ctx, q1)
	exists2, _ := qm.Exists(ctx, q2)
	if !exists2 {
		t.Error("staging/orders should still exist after deleting production/orders")
	}
}

// Scenario: List queues in namespace after deletion
func TestQueue_ListAfterDeletingNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := publicqueue.MustQueueParamsWithNS("test-ns-del-q", "temp-queue-ns")
	testutil.CreateQueue(t, ctx, q1, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	nm := namespace.NewManager()
	nm.Delete(ctx, "temp-queue-ns")

	queues, err := redissmq.NewQueueManager().ListByNamespace(ctx, "temp-queue-ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(queues) != 0 {
		t.Errorf("expected 0 queues after namespace delete, got %d", len(queues))
	}
}

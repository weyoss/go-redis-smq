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

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/namespace"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: List all queues when none exist
func TestDiscovery_ListAll_Empty(t *testing.T) {
	ctx := testutil.Setup(t)

	_, err := redissmq.NewQueueManager().ListAll(ctx)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
}

// Scenario: List all queues across namespaces
func TestDiscovery_ListAll(t *testing.T) {
	ctx := testutil.Setup(t)

	ns1 := publicqueue.MustQueueParamsWithNS("discovery-a", "ns1")
	ns2 := publicqueue.MustQueueParamsWithNS("discovery-b", "ns2")
	testutil.CreateQueue(t, ctx, ns1, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, ns2, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	all, err := redissmq.NewQueueManager().ListAll(ctx)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}

	found := make(map[string]bool)
	for _, p := range all {
		found[p.String()] = true
	}
	if !found[ns1.String()] {
		t.Errorf("missing queue: %s", ns1.String())
	}
	if !found[ns2.String()] {
		t.Errorf("missing queue: %s", ns2.String())
	}
}

// Scenario: List queues by namespace
func TestDiscovery_ListByNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	ns := publicqueue.MustQueueParamsWithNS("discovery-c", "test-ns")
	testutil.CreateQueue(t, ctx, ns, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	queues, err := redissmq.NewQueueManager().ListByNamespace(ctx, "test-ns")
	if err != nil {
		t.Fatalf("list by namespace: %v", err)
	}

	found := false
	for _, p := range queues {
		if p.String() == ns.String() {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("queue %s not found in namespace test-ns", ns.String())
	}
}

// Scenario: List queues by namespace with no queues
func TestDiscovery_ListByNamespace_Empty(t *testing.T) {
	ctx := testutil.Setup(t)

	queues, err := redissmq.NewQueueManager().ListByNamespace(ctx, "empty-ns")
	if err != nil {
		t.Fatalf("list by namespace: %v", err)
	}
	if len(queues) != 0 {
		t.Fatalf("expected 0 queues, got %d", len(queues))
	}
}

// Scenario: List namespaces
func TestDiscovery_ListNamespaces(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := publicqueue.MustQueueParamsWithNS("discovery-d", "alpha")
	q2 := publicqueue.MustQueueParamsWithNS("discovery-e", "beta")
	testutil.CreateQueue(t, ctx, q1, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	nm := namespace.NewManager()
	namespaces, err := nm.List(ctx)
	if err != nil {
		t.Fatalf("list namespaces: %v", err)
	}

	foundAlpha, foundBeta := false, false
	for _, ns := range namespaces {
		if ns == "alpha" {
			foundAlpha = true
		}
		if ns == "beta" {
			foundBeta = true
		}
	}
	if !foundAlpha {
		t.Error("namespace 'alpha' not found")
	}
	if !foundBeta {
		t.Error("namespace 'beta' not found")
	}
}

// Scenario: Delete namespace removes all queues
func TestDiscovery_DeleteNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := publicqueue.MustQueueParamsWithNS("discovery-f", "deletable-ns")
	q2 := publicqueue.MustQueueParamsWithNS("discovery-g", "deletable-ns")
	testutil.CreateQueue(t, ctx, q1, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	nm := namespace.NewManager()
	err := nm.Delete(ctx, "deletable-ns")
	if err != nil {
		t.Fatalf("delete namespace: %v", err)
	}

	queues, err := redissmq.NewQueueManager().ListByNamespace(ctx, "deletable-ns")
	if err != nil {
		t.Fatalf("list by namespace: %v", err)
	}
	if len(queues) != 0 {
		t.Fatalf("expected 0 queues after namespace delete, got %d", len(queues))
	}
}

// Scenario: Delete non-existent namespace returns error
func TestDiscovery_DeleteNamespace_NotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := namespace.NewManager()
	err := nm.Delete(ctx, "nonexistent-ns")
	if err == nil {
		t.Fatal("expected error for non-existent namespace")
	}
}

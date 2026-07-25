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
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/namespace"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Full namespace lifecycle
func TestComplex_FullLifecycle(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := namespace.NewManager()

	// Create resources in namespace
	q1 := q.MustQueueParamsWithNS("lifecycle-q", "lifecycle-ns")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)

	ex1 := x.MustExchangeParamsWithNS("lifecycle-ex", "lifecycle-ns", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)
	dx.Create(ctx, ex1, x.PolicyStandard)

	// Verify namespace exists
	exists, _ := nm.Exists(ctx, "lifecycle-ns")
	if !exists {
		t.Fatal("namespace should exist")
	}

	// List namespaces includes it
	namespaces, _ := nm.List(ctx)
	found := false
	for _, ns := range namespaces {
		if ns == "lifecycle-ns" {
			found = true
			break
		}
	}
	if !found {
		t.Error("namespace not found in list")
	}

	// List queues in namespace
	queues, _ := nm.ListQueues(ctx, "lifecycle-ns")
	if len(queues) != 1 {
		t.Errorf("expected 1 queue, got %d", len(queues))
	}

	// List exchanges in namespace
	exchanges, _ := nm.ListExchanges(ctx, "lifecycle-ns")
	if len(exchanges) != 1 {
		t.Errorf("expected 1 exchange, got %d", len(exchanges))
	}

	// Delete namespace
	err := nm.Delete(ctx, "lifecycle-ns")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify everything is gone
	exists, _ = nm.Exists(ctx, "lifecycle-ns")
	if exists {
		t.Error("namespace should not exist after delete")
	}

	queues, _ = queue.ListByNamespace(ctx, "lifecycle-ns")
	if len(queues) != 0 {
		t.Error("queues should be gone")
	}

	em := exchange.NewManager()
	exchanges, _ = em.ListByNamespace(ctx, "lifecycle-ns")
	if len(exchanges) != 0 {
		t.Error("exchanges should be gone")
	}
}

// Scenario: Multiple namespaces with mixed resources
func TestComplex_MultipleNamespaces(t *testing.T) {
	ctx := testutil.Setup(t)

	// Namespace 1: queues only
	q1 := q.MustQueueParamsWithNS("multi-q1", "ns-with-queues")
	q2 := q.MustQueueParamsWithNS("multi-q2", "ns-with-queues")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeLIFO, q.DeliveryPointToPoint)

	// Namespace 2: exchanges only
	ex1 := x.MustExchangeParamsWithNS("multi-ex1", "ns-with-exchanges", x.TypeDirect)
	ex2 := x.MustExchangeParamsWithNS("multi-ex2", "ns-with-exchanges", x.TypeFanout)
	dx := exchange.NewDirectExchange(nil)
	fx := exchange.NewFanoutExchange(nil)
	dx.Create(ctx, ex1, x.PolicyStandard)
	fx.Create(ctx, ex2, x.PolicyStandard)

	// Namespace 3: both
	q3 := q.MustQueueParamsWithNS("multi-q3", "ns-with-both")
	testutil.CreateQueue(t, ctx, q3, q.TypeFIFO, q.DeliveryPointToPoint)
	ex3 := x.MustExchangeParamsWithNS("multi-ex3", "ns-with-both", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)
	tx.Create(ctx, ex3, x.PolicyStandard)

	nm := namespace.NewManager()

	// Verify all three namespaces exist
	namespaces, _ := nm.List(ctx)
	found := make(map[string]bool)
	for _, ns := range namespaces {
		found[ns] = true
	}
	for _, expected := range []string{"ns-with-queues", "ns-with-exchanges", "ns-with-both"} {
		if !found[expected] {
			t.Errorf("namespace %s not found", expected)
		}
	}

	// Verify queue counts
	queues1, _ := nm.ListQueues(ctx, "ns-with-queues")
	queues3, _ := nm.ListQueues(ctx, "ns-with-both")
	if len(queues1) != 2 {
		t.Errorf("ns-with-queues: %d queues, want 2", len(queues1))
	}
	if len(queues3) != 1 {
		t.Errorf("ns-with-both: %d queues, want 1", len(queues3))
	}

	// Verify exchange counts
	exchanges2, _ := nm.ListExchanges(ctx, "ns-with-exchanges")
	exchanges3, _ := nm.ListExchanges(ctx, "ns-with-both")
	if len(exchanges2) != 2 {
		t.Errorf("ns-with-exchanges: %d exchanges, want 2", len(exchanges2))
	}
	if len(exchanges3) != 1 {
		t.Errorf("ns-with-both: %d exchanges, want 1", len(exchanges3))
	}
}

// Scenario: Cross-namespace isolation
func TestComplex_CrossNamespaceIsolation(t *testing.T) {
	ctx := testutil.Setup(t)

	// Same name, different namespaces
	q1 := q.MustQueueParamsWithNS("shared-name", "iso-ns-a")
	q2 := q.MustQueueParamsWithNS("shared-name", "iso-ns-b")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeLIFO, q.DeliveryPointToPoint)

	ex1 := x.MustExchangeParamsWithNS("shared-name", "iso-ns-a", x.TypeDirect)
	ex2 := x.MustExchangeParamsWithNS("shared-name", "iso-ns-b", x.TypeFanout)
	dx := exchange.NewDirectExchange(nil)
	fx := exchange.NewFanoutExchange(nil)
	dx.Create(ctx, ex1, x.PolicyStandard)
	fx.Create(ctx, ex2, x.PolicyStandard)

	// Delete namespace A only
	nm := namespace.NewManager()
	nm.Delete(ctx, "iso-ns-a")

	// Namespace B should still have its resources
	exists, _ := nm.Exists(ctx, "iso-ns-b")
	if !exists {
		t.Fatal("iso-ns-b should still exist")
	}

	queues, _ := nm.ListQueues(ctx, "iso-ns-b")
	if len(queues) != 1 {
		t.Errorf("iso-ns-b should have 1 queue, got %d", len(queues))
	}

	exchanges, _ := nm.ListExchanges(ctx, "iso-ns-b")
	if len(exchanges) != 1 {
		t.Errorf("iso-ns-b should have 1 exchange, got %d", len(exchanges))
	}

	// Namespace A should be gone
	exists, _ = nm.Exists(ctx, "iso-ns-a")
	if exists {
		t.Error("iso-ns-a should be deleted")
	}
}

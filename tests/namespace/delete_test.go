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

// Scenario: Delete namespace with queues
func TestDelete_WithQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := q.MustQueueParamsWithNS("test-del-q1", "deletable-ns")
	q2 := q.MustQueueParamsWithNS("test-del-q2", "deletable-ns")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeFIFO, q.DeliveryPointToPoint)

	nm := namespace.NewManager()
	err := nm.Delete(ctx, "deletable-ns")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Queues should be gone
	queues, err := queue.ListByNamespace(ctx, "deletable-ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(queues) != 0 {
		t.Errorf("expected 0 queues, got %d", len(queues))
	}
}

// Scenario: Delete namespace with exchanges
func TestDelete_WithExchanges(t *testing.T) {
	ctx := testutil.Setup(t)

	ex1 := x.MustExchangeParamsWithNS("test-del-ex1", "exchange-del-ns", x.TypeDirect)
	ex2 := x.MustExchangeParamsWithNS("test-del-ex2", "exchange-del-ns", x.TypeFanout)
	dx := exchange.NewDirectExchange(nil)
	fx := exchange.NewFanoutExchange(nil)
	dx.Create(ctx, ex1, x.PolicyStandard)
	fx.Create(ctx, ex2, x.PolicyStandard)

	nm := namespace.NewManager()
	err := nm.Delete(ctx, "exchange-del-ns")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Exchanges should be gone
	em := exchange.NewManager()
	exchanges, err := em.ListByNamespace(ctx, "exchange-del-ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(exchanges) != 0 {
		t.Errorf("expected 0 exchanges, got %d", len(exchanges))
	}
}

// Scenario: Delete namespace with both queues and exchanges
func TestDelete_WithQueuesAndExchanges(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParamsWithNS("test-del-mixed-q", "mixed-ns")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	exParams := x.MustExchangeParamsWithNS("test-del-mixed-ex", "mixed-ns", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)
	dx.Create(ctx, exParams, x.PolicyStandard)

	nm := namespace.NewManager()
	err := nm.Delete(ctx, "mixed-ns")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Both should be gone
	queues, _ := queue.ListByNamespace(ctx, "mixed-ns")
	exchanges, _ := exchange.NewManager().ListByNamespace(ctx, "mixed-ns")

	if len(queues) != 0 {
		t.Errorf("expected 0 queues, got %d", len(queues))
	}
	if len(exchanges) != 0 {
		t.Errorf("expected 0 exchanges, got %d", len(exchanges))
	}
}

// Scenario: Delete non-existent namespace returns error
func TestDelete_NotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := namespace.NewManager()
	err := nm.Delete(ctx, "nonexistent-ns")
	if err == nil {
		t.Fatal("expected error for non-existent namespace")
	}
}

// Scenario: Delete namespace with empty name returns error
func TestDelete_EmptyName(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := namespace.NewManager()
	err := nm.Delete(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty namespace name")
	}
}

// Scenario: Delete default namespace
func TestDelete_DefaultNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	// Create a queue in default namespace
	params := q.MustQueueParams("test-del-default")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	nm := namespace.NewManager()
	err := nm.Delete(ctx, "default")
	if err != nil {
		t.Fatalf("delete default namespace: %v", err)
	}

	// Queue should be gone
	exists, _ := queue.Exists(ctx, params)
	if exists {
		t.Error("queue in default namespace should be deleted")
	}
}

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
)

// Scenario: Create exchange in namespace
func TestExchange_CreateInNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	exParams := x.MustExchangeParamsWithNS("test-ns-ex", "exchange-ns", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	err := dx.Create(ctx, exParams, x.PolicyStandard)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if exParams.Namespace() != "exchange-ns" {
		t.Errorf("ns = %q, want %q", exParams.Namespace(), "exchange-ns")
	}
}

// Scenario: List exchanges by namespace
func TestExchange_ListByNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	ex1 := x.MustExchangeParamsWithNS("test-ns-list-ex1", "list-ex-ns", x.TypeDirect)
	ex2 := x.MustExchangeParamsWithNS("test-ns-list-ex2", "list-ex-ns", x.TypeTopic)
	dx := exchange.NewDirectExchange()
	tx := exchange.NewTopicExchange()
	dx.Create(ctx, ex1, x.PolicyStandard)
	tx.Create(ctx, ex2, x.PolicyStandard)

	em := exchange.NewManager()
	exchanges, err := em.ListByNamespace(ctx, "list-ex-ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(exchanges) != 2 {
		t.Fatalf("expected 2 exchanges, got %d", len(exchanges))
	}
}

// Scenario: Exchanges in different namespaces are isolated
func TestExchange_NamespaceIsolation(t *testing.T) {
	ctx := testutil.Setup(t)

	ex1 := x.MustExchangeParamsWithNS("same-name", "ex-ns-a", x.TypeDirect)
	ex2 := x.MustExchangeParamsWithNS("same-name", "ex-ns-b", x.TypeDirect)
	dx1 := exchange.NewDirectExchange()
	dx2 := exchange.NewDirectExchange()
	dx1.Create(ctx, ex1, x.PolicyStandard)
	dx2.Create(ctx, ex2, x.PolicyStandard)

	// Both should exist independently
	em := exchange.NewManager()
	exists1, _ := em.Exists(ctx, ex1)
	exists2, _ := em.Exists(ctx, ex2)

	if !exists1 {
		t.Error("exchange in ex-ns-a should exist")
	}
	if !exists2 {
		t.Error("exchange in ex-ns-b should exist")
	}
}

// Scenario: Same exchange name in different namespaces
func TestExchange_SameNameDifferentNamespaces(t *testing.T) {
	ctx := testutil.Setup(t)

	ex1 := x.MustExchangeParamsWithNS("events", "production", x.TypeTopic)
	ex2 := x.MustExchangeParamsWithNS("events", "staging", x.TypeTopic)
	tx1 := exchange.NewTopicExchange()
	tx2 := exchange.NewTopicExchange()
	tx1.Create(ctx, ex1, x.PolicyStandard)
	tx2.Create(ctx, ex2, x.PolicyStandard)

	// Delete one should not affect the other
	tx1.Delete(ctx, ex1)

	em := exchange.NewManager()
	exists2, _ := em.Exists(ctx, ex2)
	if !exists2 {
		t.Error("staging/events should still exist after deleting production/events")
	}
}

// Scenario: Bind queue to exchange in same namespace
func TestExchange_BindInNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParamsWithNS("test-ns-bind-q", "bind-ns")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exParams := x.MustExchangeParamsWithNS("test-ns-bind-ex", "bind-ns", x.TypeDirect)
	dx := exchange.NewDirectExchange()

	err := dx.BindQueue(ctx, queueParams, exParams, "test.key")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
}

// Scenario: List exchanges after deleting namespace
func TestExchange_ListAfterDeletingNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	ex1 := x.MustExchangeParamsWithNS("test-ns-del-ex", "temp-ex-ns", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	dx.Create(ctx, ex1, x.PolicyStandard)

	nm := namespace.NewManager()
	nm.Delete(ctx, "temp-ex-ns")

	em := exchange.NewManager()
	exchanges, err := em.ListByNamespace(ctx, "temp-ex-ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(exchanges) != 0 {
		t.Errorf("expected 0 exchanges after namespace delete, got %d", len(exchanges))
	}
}

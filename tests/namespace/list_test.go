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
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: List namespaces when none exist
func TestList_Empty(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := redissmq.NewNamespaceManager()
	namespaces, err := nm.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	// May have default namespace from system init
	t.Logf("namespaces: %v", namespaces)
}

// Scenario: List namespaces with queues in different namespaces
func TestList_WithQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := queue.MustQueueParamsWithNS("test-list-q1", "ns-alpha")
	q2 := queue.MustQueueParamsWithNS("test-list-q2", "ns-beta")
	testutil.CreateQueue(t, ctx, q1, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, queue.TypeFIFO, queue.DeliveryPointToPoint)

	nm := redissmq.NewNamespaceManager()
	namespaces, err := nm.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	found := make(map[string]bool)
	for _, ns := range namespaces {
		found[ns] = true
	}
	if !found["ns-alpha"] {
		t.Error("namespace ns-alpha not found")
	}
	if !found["ns-beta"] {
		t.Error("namespace ns-beta not found")
	}
}

// Scenario: List namespaces with exchanges
func TestList_WithExchanges(t *testing.T) {
	ctx := testutil.Setup(t)

	ex1 := exchange.MustExchangeParamsWithNS("test-list-ex1", "ns-exchange-alpha", exchange.TypeDirect)
	ex2 := exchange.MustExchangeParamsWithNS("test-list-ex2", "ns-exchange-beta", exchange.TypeFanout)

	dx := redissmq.NewDirectExchange()
	fx := redissmq.NewFanoutExchange()
	dx.Create(ctx, ex1, exchange.PolicyStandard)
	fx.Create(ctx, ex2, exchange.PolicyStandard)

	nm := redissmq.NewNamespaceManager()
	namespaces, err := nm.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	found := make(map[string]bool)
	for _, ns := range namespaces {
		found[ns] = true
	}
	if !found["ns-exchange-alpha"] {
		t.Error("namespace ns-exchange-alpha not found")
	}
	if !found["ns-exchange-beta"] {
		t.Error("namespace ns-exchange-beta not found")
	}
}

// Scenario: List namespaces after deleting one
func TestList_AfterDelete(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := queue.MustQueueParamsWithNS("test-list-del-q", "ns-to-keep")
	q2 := queue.MustQueueParamsWithNS("test-list-del-q2", "ns-to-delete")
	testutil.CreateQueue(t, ctx, q1, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, queue.TypeFIFO, queue.DeliveryPointToPoint)

	nm := redissmq.NewNamespaceManager()
	nm.Delete(ctx, "ns-to-delete")

	namespaces, err := nm.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	found := make(map[string]bool)
	for _, ns := range namespaces {
		found[ns] = true
	}
	if !found["ns-to-keep"] {
		t.Error("namespace ns-to-keep should still exist")
	}
	if found["ns-to-delete"] {
		t.Error("namespace ns-to-delete should be gone")
	}
}

// Scenario: List namespaces with mixed resources
func TestList_MixedResources(t *testing.T) {
	ctx := testutil.Setup(t)

	// Queue in one namespace
	q1 := queue.MustQueueParamsWithNS("test-list-mixed-q", "ns-mixed")
	testutil.CreateQueue(t, ctx, q1, queue.TypeFIFO, queue.DeliveryPointToPoint)

	// Exchange in same namespace
	ex1 := exchange.MustExchangeParamsWithNS("test-list-mixed-ex", "ns-mixed", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.Create(ctx, ex1, exchange.PolicyStandard)

	nm := redissmq.NewNamespaceManager()
	namespaces, err := nm.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	found := false
	for _, ns := range namespaces {
		if ns == "ns-mixed" {
			found = true
			break
		}
	}
	if !found {
		t.Error("namespace ns-mixed not found")
	}
}

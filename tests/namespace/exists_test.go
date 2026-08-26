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

// Scenario: Exists returns true for namespace with queues
func TestExists_WithQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParamsWithNS("test-exists-q", "existing-ns")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	nm := redissmq.NewNamespaceManager()
	exists, err := nm.Exists(ctx, "existing-ns")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Error("namespace should exist")
	}
}

// Scenario: Exists returns true for namespace with exchanges
func TestExists_WithExchanges(t *testing.T) {
	ctx := testutil.Setup(t)

	exParams := exchange.MustExchangeParamsWithNS("test-exists-ex", "exchange-ns", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.Create(ctx, exParams, exchange.PolicyStandard)

	nm := redissmq.NewNamespaceManager()
	exists, err := nm.Exists(ctx, "exchange-ns")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Error("namespace should exist")
	}
}

// Scenario: Exists returns false for non-existent namespace
func TestExists_NotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := redissmq.NewNamespaceManager()
	exists, err := nm.Exists(ctx, "nonexistent-ns")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists {
		t.Error("namespace should not exist")
	}
}

// Scenario: Exists returns false after namespace deleted
func TestExists_AfterDelete(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParamsWithNS("test-exists-del-q", "temp-ns")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	nm := redissmq.NewNamespaceManager()
	nm.Delete(ctx, "temp-ns")

	exists, err := nm.Exists(ctx, "temp-ns")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists {
		t.Error("namespace should not exist after delete")
	}
}

// Scenario: Exists with empty name returns error
func TestExists_EmptyName(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := redissmq.NewNamespaceManager()
	_, err := nm.Exists(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty namespace name")
	}
}

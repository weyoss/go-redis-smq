/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package exchange_test

import (
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Type mismatch — direct operation on topic exchange
func TestError_TypeMismatch(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-error-type-mismatch-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	// Create as topic
	topicParams := x.MustExchangeParams("test-error-type-mismatch-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)
	tx.Create(ctx, topicParams, x.PolicyStandard)

	// Try to use direct exchange methods on it
	directParams := x.MustExchangeParams("test-error-type-mismatch-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	_, err := dx.MatchQueues(ctx, directParams, "test.key")
	if err == nil {
		t.Fatal("expected type mismatch error")
	}
	t.Logf("type mismatch error: %v", err)
}

// Scenario: Non-existent exchange
func TestError_ExchangeNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := x.MustExchangeParams("nonexistent", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	_, err := dx.MatchQueues(ctx, params, "test.key")
	if err == nil {
		t.Fatal("expected error for non-existent exchange")
	}
}

// Scenario: Unbind non-existent binding
func TestError_UnbindNotBound(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-error-unbind-not-bound-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-error-unbind-not-bound-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)
	dx.Create(ctx, exchangeParams, x.PolicyStandard)

	err := dx.UnbindQueue(ctx, queueParams, exchangeParams, "never.bound")
	if err == nil {
		t.Fatal("expected error: queue not bound")
	}
}

// Scenario: Create duplicate exchange
func TestError_DuplicateExchange(t *testing.T) {
	ctx := testutil.Setup(t)

	params := x.MustExchangeParams("test-error-dup-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	err := dx.Create(ctx, params, x.PolicyStandard)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	err = dx.Create(ctx, params, x.PolicyStandard)
	if err == nil {
		t.Fatal("expected error for duplicate exchange")
	}
}

// Scenario: Cross-namespace binding
func TestError_CrossNamespaceBinding(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParamsWithNS("test-error-cross-ns-q", "ns1")
	exchangeParams := x.MustExchangeParamsWithNS("test-error-cross-ns-ex", "ns2", x.TypeDirect)

	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	dx := exchange.NewDirectExchange(nil)
	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("expected error: namespace mismatch")
	}
}

// Scenario: Delete non-existent exchange
func TestError_DeleteNonExistent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := x.MustExchangeParams("nonexistent-delete", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	err := dx.Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error: exchange not found")
	}
}

// Scenario: Empty routing key on direct exchange
func TestError_EmptyRoutingKey(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-error-empty-rk-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-error-empty-rk-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "")
	if err == nil {
		t.Log("empty routing key allowed")
	}
}

// Scenario: Invalid exchange name
func TestError_InvalidExchangeName(t *testing.T) {
	_, err := x.NewExchangeParams("", x.TypeDirect)
	if err == nil {
		t.Fatal("expected error for empty exchange name")
	}

	_, err = x.NewExchangeParams("3invalid", x.TypeDirect)
	if err == nil {
		t.Fatal("expected error: name starts with number")
	}
}

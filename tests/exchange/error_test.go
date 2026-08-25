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

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Type mismatch — direct operation on topic exchange
func TestError_TypeMismatch(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-error-type-mismatch-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	// Create as topic
	topicParams := exchange.MustExchangeParams("test-error-type-mismatch-ex", exchange.TypeTopic)
	tx := redissmq.NewTopicExchange()
	tx.Create(ctx, topicParams, exchange.PolicyStandard)

	// Try to use direct exchange methods on it
	directParams := exchange.MustExchangeParams("test-error-type-mismatch-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	_, err := dx.MatchQueues(ctx, directParams, "test.key")
	if err == nil {
		t.Fatal("expected type mismatch error")
	}
	t.Logf("type mismatch error: %v", err)
}

// Scenario: Non-existent exchange
func TestError_ExchangeNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := exchange.MustExchangeParams("nonexistent", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	_, err := dx.MatchQueues(ctx, params, "test.key")
	if err == nil {
		t.Fatal("expected error for non-existent exchange")
	}
}

// Scenario: Unbind non-existent binding
func TestError_UnbindNotBound(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-error-unbind-not-bound-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-error-unbind-not-bound-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.Create(ctx, exchangeParams, exchange.PolicyStandard)

	err := dx.UnbindQueue(ctx, queueParams, exchangeParams, "never.bound")
	if err == nil {
		t.Fatal("expected error: queue not bound")
	}
}

// Scenario: Create duplicate exchange
func TestError_DuplicateExchange(t *testing.T) {
	ctx := testutil.Setup(t)

	params := exchange.MustExchangeParams("test-error-dup-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	err := dx.Create(ctx, params, exchange.PolicyStandard)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	err = dx.Create(ctx, params, exchange.PolicyStandard)
	if err == nil {
		t.Fatal("expected error for duplicate exchange")
	}
}

// Scenario: Cross-namespace binding
func TestError_CrossNamespaceBinding(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParamsWithNS("test-error-cross-ns-q", "ns1")
	exchangeParams := exchange.MustExchangeParamsWithNS("test-error-cross-ns-ex", "ns2", exchange.TypeDirect)

	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	dx := redissmq.NewDirectExchange()
	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("expected error: namespace mismatch")
	}
}

// Scenario: Delete non-existent exchange
func TestError_DeleteNonExistent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := exchange.MustExchangeParams("nonexistent-delete", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	err := dx.Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error: exchange not found")
	}
}

// Scenario: Empty routing key on direct exchange
func TestError_EmptyRoutingKey(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-error-empty-rk-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-error-empty-rk-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "")
	if err == nil {
		t.Log("empty routing key allowed")
	}
}

// Scenario: Invalid exchange name
func TestError_InvalidExchangeName(t *testing.T) {
	_, err := exchange.NewExchangeParams("", exchange.TypeDirect)
	if err == nil {
		t.Fatal("expected error for empty exchange name")
	}

	_, err = exchange.NewExchangeParams("3invalid", exchange.TypeDirect)
	if err == nil {
		t.Fatal("expected error: name starts with number")
	}
}

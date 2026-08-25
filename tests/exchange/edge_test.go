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
	"fmt"
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Rapid create/delete cycles on exchange
func TestEdge_RapidCreateDelete(t *testing.T) {
	ctx := testutil.Setup(t)

	for i := 0; i < 10; i++ {
		params := exchange.MustExchangeParams("test-edge-rapid", exchange.TypeDirect)
		dx := redissmq.NewDirectExchange()

		err := dx.Create(ctx, params, exchange.PolicyStandard)
		if err != nil {
			t.Fatalf("cycle %d create: %v", i, err)
		}

		err = dx.Delete(ctx, params)
		if err != nil {
			t.Fatalf("cycle %d delete: %v", i, err)
		}
	}
}

// Scenario: Bind/unbind cycle
func TestEdge_BindUnbindCycle(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-edge-bind-cycle-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-edge-bind-cycle-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	for i := 0; i < 10; i++ {
		err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
		if err != nil && i > 0 {
			// First bind succeeds, subsequent are duplicates
			t.Logf("cycle %d bind: %v (expected duplicate)", i, err)
		}

		if i%2 == 0 {
			dx.UnbindQueue(ctx, queueParams, exchangeParams, "test.key")
		}
	}
}

// Scenario: Very long routing key
func TestEdge_LongRoutingKey(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-edge-long-rk-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-edge-long-rk-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	longKey := "this.is.a.very.long.routing.key.that.tests.the.limits.of.redis.key.storage.and.should.still.work.correctly"
	err := dx.BindQueue(ctx, queueParams, exchangeParams, longKey)
	if err != nil {
		t.Fatalf("bind with long key: %v", err)
	}

	queues, err := dx.MatchQueues(ctx, exchangeParams, longKey)
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(queues) != 1 {
		t.Errorf("expected 1 queue, got %d", len(queues))
	}
}

// Scenario: Many bindings on a single exchange
func TestEdge_ManyBindings(t *testing.T) {
	ctx := testutil.Setup(t)

	exchangeParams := exchange.MustExchangeParams("test-edge-many-bindings-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	queueCount := 20
	for i := 0; i < queueCount; i++ {
		queueParams := queue.MustQueueParams(fmt.Sprintf("test-edge-many-bindings-q%d", i))
		testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

		routingKey := fmt.Sprintf("key.%d", i)
		err := dx.BindQueue(ctx, queueParams, exchangeParams, routingKey)
		if err != nil {
			t.Fatalf("bind %d: %v", i, err)
		}
	}

	keys, err := dx.RoutingKeys(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("routing keys: %v", err)
	}
	if len(keys) != queueCount {
		t.Errorf("routing keys: %d, want %d", len(keys), queueCount)
	}
}

// Scenario: Routing key with special characters
func TestEdge_RoutingKeySpecialChars(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-edge-special-rk-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-edge-special-rk-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	specialKeys := []string{
		"order.created",
		"order.cancelled.v2",
		"user.profile.updated",
		"system.health-check",
		"api.v1.users.get",
	}

	for _, key := range specialKeys {
		err := dx.BindQueue(ctx, queueParams, exchangeParams, key)
		if err != nil {
			t.Fatalf("bind %s: %v", key, err)
		}

		queues, _ := dx.MatchQueues(ctx, exchangeParams, key)
		if len(queues) != 1 {
			t.Errorf("key %s matched %d queues, want 1", key, len(queues))
		}
	}
}

// Scenario: Exchange name with namespace
func TestEdge_ExchangeWithNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParamsWithNS("test-edge-ns-q", "production")
	exchangeParams := exchange.MustExchangeParamsWithNS("test-edge-ns-ex", "production", exchange.TypeDirect)

	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	dx := redissmq.NewDirectExchange()
	err := dx.BindQueue(ctx, queueParams, exchangeParams, "order.created")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Exchange should be listed in the namespace
	em := redissmq.NewExchangeManager()
	exchanges, err := em.ListByNamespace(ctx, "production")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	found := false
	for _, e := range exchanges {
		if e.String() == exchangeParams.String() {
			found = true
			break
		}
	}
	if !found {
		t.Error("exchange not found in namespace")
	}
}

// Scenario: Topic pattern with many tokens
func TestEdge_TopicManyTokens(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-edge-many-tokens-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-edge-many-tokens-ex", exchange.TypeTopic)
	tx := redissmq.NewTopicExchange()

	// Pattern with multiple wildcards
	tx.BindQueue(ctx, queueParams, exchangeParams, "a.*.c.*.e.#")

	// Should match
	queues, _ := tx.MatchQueues(ctx, exchangeParams, "a.b.c.d.e.f.g")
	if len(queues) != 1 {
		t.Errorf("complex pattern should match, got %d queues", len(queues))
	}
}

// Scenario: Fanout with zero bound queues
func TestEdge_FanoutNoQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	exchangeParams := exchange.MustExchangeParams("test-edge-fanout-empty", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()
	fx.Create(ctx, exchangeParams, exchange.PolicyStandard)

	queues, err := fx.BoundQueues(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("bound queues: %v", err)
	}
	if len(queues) != 0 {
		t.Errorf("expected 0 queues, got %d", len(queues))
	}
}

// Scenario: Delete exchange by type mismatch
func TestEdge_DeleteByType(t *testing.T) {
	ctx := testutil.Setup(t)

	// Create as direct
	params := exchange.MustExchangeParams("test-edge-delete-type", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.Create(ctx, params, exchange.PolicyStandard)

	// Try to delete as fanout — should fail
	fx := redissmq.NewFanoutExchange()
	fanoutParams := exchange.MustExchangeParams("test-edge-delete-type", exchange.TypeFanout)
	err := fx.Delete(ctx, fanoutParams)
	if err == nil {
		t.Fatal("expected type mismatch error when deleting with wrong type")
	}
}

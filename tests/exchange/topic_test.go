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
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Create a topic exchange
func TestTopic_Create(t *testing.T) {
	ctx := testutil.Setup(t)

	exchangeParams := x.MustExchangeParams("test-topic-create", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)

	err := tx.Create(ctx, exchangeParams, x.PolicyStandard)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
}

// Scenario: Bind queue with wildcard pattern
func TestTopic_BindQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-topic-bind-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-topic-bind-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)

	err := tx.BindQueue(ctx, queueParams, exchangeParams, "user.*")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
}

// Scenario: * matches exactly one token
func TestTopic_SingleWildcard(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-topic-star-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-topic-star-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)
	tx.BindQueue(ctx, queueParams, exchangeParams, "user.*")

	// Should match
	queues, _ := tx.MatchQueues(ctx, exchangeParams, "user.created")
	if len(queues) != 1 {
		t.Errorf("'user.created' should match 'user.*', got %d queues", len(queues))
	}

	// Should NOT match (two tokens after user)
	queues, _ = tx.MatchQueues(ctx, exchangeParams, "user.profile.updated")
	if len(queues) != 0 {
		t.Errorf("'user.profile.updated' should NOT match 'user.*', got %d queues", len(queues))
	}

	// Should NOT match (no dot)
	queues, _ = tx.MatchQueues(ctx, exchangeParams, "user")
	if len(queues) != 0 {
		t.Errorf("'user' should NOT match 'user.*', got %d queues", len(queues))
	}
}

// Scenario: # matches zero or more tokens
func TestTopic_HashWildcard(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-topic-hash-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-topic-hash-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)
	tx.BindQueue(ctx, queueParams, exchangeParams, "user.#")

	// Should match — one token
	queues, _ := tx.MatchQueues(ctx, exchangeParams, "user")
	if len(queues) != 1 {
		t.Errorf("'user' should match 'user.#', got %d queues", len(queues))
	}

	// Should match — multiple tokens
	queues, _ = tx.MatchQueues(ctx, exchangeParams, "user.profile.updated")
	if len(queues) != 1 {
		t.Errorf("'user.profile.updated' should match 'user.#', got %d queues", len(queues))
	}

	// Should NOT match — different root
	queues, _ = tx.MatchQueues(ctx, exchangeParams, "order.created")
	if len(queues) != 0 {
		t.Errorf("'order.created' should NOT match 'user.#', got %d queues", len(queues))
	}
}

// Scenario: # at the beginning matches everything
func TestTopic_HashAtBeginning(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-topic-hash-begin-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-topic-hash-begin-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)
	tx.BindQueue(ctx, queueParams, exchangeParams, "#")

	queues, _ := tx.MatchQueues(ctx, exchangeParams, "anything.at.all")
	if len(queues) != 1 {
		t.Errorf("'anything.at.all' should match '#', got %d queues", len(queues))
	}
}

// Scenario: Multiple patterns match the same routing key
func TestTopic_MultipleMatches(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := q.MustQueueParams("test-topic-multi-q1")
	q2 := q.MustQueueParams("test-topic-multi-q2")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-topic-multi-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)

	tx.BindQueue(ctx, q1, exchangeParams, "user.*")
	tx.BindQueue(ctx, q2, exchangeParams, "*.created")

	// Should match both patterns
	queues, _ := tx.MatchQueues(ctx, exchangeParams, "user.created")
	if len(queues) != 2 {
		t.Errorf("'user.created' should match both patterns, got %d queues", len(queues))
	}
}

// Scenario: Invalid pattern returns error
func TestTopic_InvalidPattern(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-topic-invalid-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-topic-invalid-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)

	err := tx.BindQueue(ctx, queueParams, exchangeParams, "invalid..pattern")
	if err == nil {
		t.Fatal("expected error for invalid pattern")
	}
}

// Scenario: List patterns
func TestTopic_Patterns(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-topic-patterns-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-topic-patterns-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)

	tx.BindQueue(ctx, queueParams, exchangeParams, "user.*")
	tx.BindQueue(ctx, queueParams, exchangeParams, "order.#")

	patterns, err := tx.Patterns(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("patterns: %v", err)
	}
	if len(patterns) != 2 {
		t.Fatalf("patterns = %d, want 2", len(patterns))
	}
}

// Scenario: Produce and consume via topic exchange
func TestTopic_ProduceConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	userQueue := q.MustQueueParams("test-topic-prod-user-q")
	orderQueue := q.MustQueueParams("test-topic-prod-order-q")
	testutil.CreateQueue(t, ctx, userQueue, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, orderQueue, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-topic-prod-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)
	tx.BindQueue(ctx, userQueue, exchangeParams, "user.*")
	tx.BindQueue(ctx, orderQueue, exchangeParams, "order.#")

	var userCount, orderCount atomic.Int64

	cons1 := redissmq.NewConsumer()
	cons1.Consume(userQueue, func(ctx context.Context, m *msg.Transferable) error {
		userCount.Add(1)
		return nil
	})
	cons1.Run(ctx)
	defer cons1.Shutdown()

	cons2 := redissmq.NewConsumer()
	cons2.Consume(orderQueue, func(ctx context.Context, m *msg.Transferable) error {
		orderCount.Add(1)
		return nil
	})
	cons2.Run(ctx)
	defer cons2.Shutdown()

	prod := testutil.StartProducer(t, ctx)

	// Should route to userQueue only
	prod.Produce(ctx, msg.New().
		SetBody("user-event").
		SetTopicExchange(exchangeParams).
		SetExchangeRoutingKey("user.created"),
	)

	// Should route to orderQueue only
	prod.Produce(ctx, msg.New().
		SetBody("order-event").
		SetTopicExchange(exchangeParams).
		SetExchangeRoutingKey("order.created"),
	)

	time.Sleep(3 * time.Second)

	if userCount.Load() != 1 {
		t.Errorf("user queue: %d, want 1", userCount.Load())
	}
	if orderCount.Load() != 1 {
		t.Errorf("order queue: %d, want 1", orderCount.Load())
	}
}

// Scenario: Unbind pattern
func TestTopic_Unbind(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-topic-unbind-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-topic-unbind-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)

	tx.BindQueue(ctx, queueParams, exchangeParams, "user.*")
	tx.UnbindQueue(ctx, queueParams, exchangeParams, "user.*")

	queues, _ := tx.MatchQueues(ctx, exchangeParams, "user.created")
	if len(queues) != 0 {
		t.Fatalf("expected 0 queues after unbind, got %d", len(queues))
	}
}

// Scenario: List bindings
func TestTopic_Bindings(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-topic-bindings-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-topic-bindings-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange(nil)

	tx.BindQueue(ctx, queueParams, exchangeParams, "user.*")
	tx.BindQueue(ctx, queueParams, exchangeParams, "order.#")

	bindings, err := tx.Bindings(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("bindings: %v", err)
	}
	if len(bindings) != 2 {
		t.Fatalf("bindings = %d, want 2", len(bindings))
	}
}

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

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Mixed exchange types routing simultaneously
func TestComplex_MixedExchangeTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	// Direct exchange
	directQueue := queue.MustQueueParams("test-complex-mixed-direct-q")
	testutil.CreateQueue(t, ctx, directQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)
	directEx := x.MustExchangeParams("test-complex-mixed-direct-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	dx.BindQueue(ctx, directQueue, directEx, "order.created")

	// Topic exchange
	topicQueue := queue.MustQueueParams("test-complex-mixed-topic-q")
	testutil.CreateQueue(t, ctx, topicQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)
	topicEx := x.MustExchangeParams("test-complex-mixed-topic-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange()
	tx.BindQueue(ctx, topicQueue, topicEx, "user.*")

	// Fanout exchange
	fanoutQ1 := queue.MustQueueParams("test-complex-mixed-fanout-q1")
	fanoutQ2 := queue.MustQueueParams("test-complex-mixed-fanout-q2")
	testutil.CreateQueue(t, ctx, fanoutQ1, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, fanoutQ2, queue.TypeFIFO, queue.DeliveryPointToPoint)
	fanoutEx := x.MustExchangeParams("test-complex-mixed-fanout-ex", x.TypeFanout)
	fx := exchange.NewFanoutExchange()
	fx.BindQueue(ctx, fanoutQ1, fanoutEx)
	fx.BindQueue(ctx, fanoutQ2, fanoutEx)

	// Consumers
	var directCount, topicCount, fanout1Count, fanout2Count atomic.Int64
	startConsumer(t, ctx, directQueue, &directCount)
	startConsumer(t, ctx, topicQueue, &topicCount)
	startConsumer(t, ctx, fanoutQ1, &fanout1Count)
	startConsumer(t, ctx, fanoutQ2, &fanout2Count)

	prod := testutil.StartProducer(t, ctx)

	// Publish via direct exchange
	prod.Produce(ctx, msg.New().
		SetBody("direct").
		SetDirectExchange(directEx).
		SetExchangeRoutingKey("order.created"),
	)

	// Publish via topic exchange
	prod.Produce(ctx, msg.New().
		SetBody("topic").
		SetTopicExchange(topicEx).
		SetExchangeRoutingKey("user.created"),
	)

	// Publish via fanout exchange
	prod.Produce(ctx, msg.New().
		SetBody("fanout").
		SetFanoutExchange(fanoutEx),
	)

	time.Sleep(3 * time.Second)

	if directCount.Load() != 1 {
		t.Errorf("direct: %d, want 1", directCount.Load())
	}
	if topicCount.Load() != 1 {
		t.Errorf("topic: %d, want 1", topicCount.Load())
	}
	if fanout1Count.Load() != 1 {
		t.Errorf("fanout1: %d, want 1", fanout1Count.Load())
	}
	if fanout2Count.Load() != 1 {
		t.Errorf("fanout2: %d, want 1", fanout2Count.Load())
	}
}

// Scenario: One queue bound to multiple exchanges
func TestComplex_OneQueueMultipleExchanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	sharedQueue := queue.MustQueueParams("test-complex-shared-q")
	testutil.CreateQueue(t, ctx, sharedQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	ex1 := x.MustExchangeParams("test-complex-shared-ex1", x.TypeDirect)
	ex2 := x.MustExchangeParams("test-complex-shared-ex2", x.TypeFanout)

	dx := exchange.NewDirectExchange()
	dx.BindQueue(ctx, sharedQueue, ex1, "order.created")

	fx := exchange.NewFanoutExchange()
	fx.BindQueue(ctx, sharedQueue, ex2)

	var count atomic.Int64
	startConsumer(t, ctx, sharedQueue, &count)

	prod := testutil.StartProducer(t, ctx)

	// Publish via direct exchange
	prod.Produce(ctx, msg.New().
		SetBody("via-direct").
		SetDirectExchange(ex1).
		SetExchangeRoutingKey("order.created"),
	)

	// Publish via fanout exchange
	prod.Produce(ctx, msg.New().
		SetBody("via-fanout").
		SetFanoutExchange(ex2),
	)

	time.Sleep(3 * time.Second)

	if count.Load() != 2 {
		t.Errorf("shared queue: %d messages, want 2", count.Load())
	}
}

// Scenario: Multiple producers publishing to the same exchange
func TestComplex_MultipleProducersSameExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	queueParams := queue.MustQueueParams("test-complex-multi-prod-ex-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-complex-multi-prod-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	dx.BindQueue(ctx, queueParams, exchangeParams, "task.process")

	var count atomic.Int64
	startConsumer(t, ctx, queueParams, &count)

	producerCount := 5
	messagesPerProducer := 10

	for i := 0; i < producerCount; i++ {
		go func() {
			prod := testutil.StartProducer(t, ctx)
			for j := 0; j < messagesPerProducer; j++ {
				prod.Produce(ctx, msg.New().
					SetBody("task").
					SetDirectExchange(exchangeParams).
					SetExchangeRoutingKey("task.process"),
				)
			}
		}()
	}

	time.Sleep(5 * time.Second)

	expected := int64(producerCount * messagesPerProducer)
	if count.Load() != expected {
		t.Errorf("consumed: %d, want %d", count.Load(), expected)
	}
}

// Scenario: Dynamic bind/unbind while producing
func TestComplex_DynamicBindUnbind(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	q1 := queue.MustQueueParams("test-complex-dynamic-q1")
	q2 := queue.MustQueueParams("test-complex-dynamic-q2")
	testutil.CreateQueue(t, ctx, q1, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-complex-dynamic-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()

	dx.BindQueue(ctx, q1, exchangeParams, "order.created")

	var c1, c2 atomic.Int64
	startConsumer(t, ctx, q1, &c1)
	startConsumer(t, ctx, q2, &c2)

	prod := testutil.StartProducer(t, ctx)

	// Batch 1: only q1 bound
	prod.Produce(ctx, msg.New().
		SetBody("batch-1").
		SetDirectExchange(exchangeParams).
		SetExchangeRoutingKey("order.created"),
	)
	time.Sleep(2 * time.Second)
	batch1Count := c1.Load()
	t.Logf("after batch 1: q1=%d, q2=%d", c1.Load(), c2.Load())

	// Bind q2
	dx.BindQueue(ctx, q2, exchangeParams, "order.created")

	// Batch 2: both bound
	prod.Produce(ctx, msg.New().
		SetBody("batch-2").
		SetDirectExchange(exchangeParams).
		SetExchangeRoutingKey("order.created"),
	)
	time.Sleep(2 * time.Second)
	t.Logf("after batch 2: q1=%d, q2=%d", c1.Load(), c2.Load())

	// Unbind q1
	dx.UnbindQueue(ctx, q1, exchangeParams, "order.created")

	// Batch 3: only q2 bound
	prod.Produce(ctx, msg.New().
		SetBody("batch-3").
		SetDirectExchange(exchangeParams).
		SetExchangeRoutingKey("order.created"),
	)
	time.Sleep(2 * time.Second)
	t.Logf("after batch 3: q1=%d, q2=%d", c1.Load(), c2.Load())

	// q1 should have received at least batch 1
	if c1.Load() < batch1Count {
		t.Errorf("q1 should have received at least %d messages, got %d", batch1Count, c1.Load())
	}
	// q2 should have received batch 2 and 3
	if c2.Load() < 2 {
		t.Errorf("q2 should have received at least 2 messages, got %d", c2.Load())
	}
}

// Scenario: Exchange discovery
func TestComplex_ExchangeDiscovery(t *testing.T) {
	ctx := testutil.Setup(t)

	ex1 := x.MustExchangeParamsWithNS("test-complex-discovery-ex1", "ns-alpha", x.TypeDirect)
	ex2 := x.MustExchangeParamsWithNS("test-complex-discovery-ex2", "ns-alpha", x.TypeTopic)
	ex3 := x.MustExchangeParamsWithNS("test-complex-discovery-ex3", "ns-beta", x.TypeFanout)

	dx := exchange.NewDirectExchange()
	tx := exchange.NewTopicExchange()
	fx := exchange.NewFanoutExchange()

	dx.Create(ctx, ex1, x.PolicyStandard)
	tx.Create(ctx, ex2, x.PolicyStandard)
	fx.Create(ctx, ex3, x.PolicyStandard)

	em := exchange.NewManager()

	// List all
	all, err := em.ListAll(ctx)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) < 3 {
		t.Errorf("all exchanges: %d, want >= 3", len(all))
	}

	// List by namespace
	alphaExchanges, err := em.ListByNamespace(ctx, "ns-alpha")
	if err != nil {
		t.Fatalf("list by ns: %v", err)
	}
	if len(alphaExchanges) != 2 {
		t.Errorf("ns-alpha exchanges: %d, want 2", len(alphaExchanges))
	}

	betaExchanges, err := em.ListByNamespace(ctx, "ns-beta")
	if err != nil {
		t.Fatalf("list by ns: %v", err)
	}
	if len(betaExchanges) != 1 {
		t.Errorf("ns-beta exchanges: %d, want 1", len(betaExchanges))
	}
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicconsumer "github.com/weyoss/go-redis-smq/pkg/consumer"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Multiple queues with different types simultaneously
func TestComplex_MultipleQueueTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	fifoQueue := publicqueue.MustQueueParams("test-complex-fifo")
	lifoQueue := publicqueue.MustQueueParams("test-complex-lifo")
	prioQueue := publicqueue.MustQueueParams("test-complex-prio")

	testutil.CreateQueue(t, ctx, fifoQueue, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, lifoQueue, publicqueue.TypeLIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, prioQueue, publicqueue.TypePriority, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	var fifoCount, lifoCount, prioCount atomic.Int64

	cons := redissmq.NewConsumer()
	cons.Consume(fifoQueue, func(ctx context.Context, m *msg.Transferable) error {
		fifoCount.Add(1)
		return nil
	})
	cons.Consume(lifoQueue, func(ctx context.Context, m *msg.Transferable) error {
		lifoCount.Add(1)
		return nil
	})
	cons.Consume(prioQueue, func(ctx context.Context, m *msg.Transferable) error {
		prioCount.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	// Produce to all queues
	prod.Produce(ctx, msg.New().SetBody("fifo").SetQueue(fifoQueue))
	prod.Produce(ctx, msg.New().SetBody("lifo").SetQueue(lifoQueue))
	prod.Produce(ctx, msg.New().SetBody("prio").SetQueue(prioQueue).SetPriority(msg.PriorityHigh))

	time.Sleep(5 * time.Second)

	if fifoCount.Load() != 1 {
		t.Errorf("fifo: %d, want 1", fifoCount.Load())
	}
	if lifoCount.Load() != 1 {
		t.Errorf("lifo: %d, want 1", lifoCount.Load())
	}
	if prioCount.Load() != 1 {
		t.Errorf("prio: %d, want 1", prioCount.Load())
	}
}

// Scenario: Consumer with pubsub and point-to-point queues simultaneously
func TestComplex_MixedDeliveryModels(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	p2pQueue := publicqueue.MustQueueParams("test-complex-p2p")
	pubsubQueue := publicqueue.MustQueueParams("test-complex-pubsub")

	testutil.CreateQueue(t, ctx, p2pQueue, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, pubsubQueue, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	redissmq.NewConsumerGroupManager().Save(ctx, pubsubQueue, "notifications")

	prod := testutil.StartProducer(t, ctx)

	var p2pCount, pubsubCount atomic.Int64

	cons := redissmq.NewConsumer()
	cons.Consume(p2pQueue, func(ctx context.Context, m *msg.Transferable) error {
		p2pCount.Add(1)
		return nil
	})
	cons.ConsumeWithGroup(pubsubQueue, "notifications", func(ctx context.Context, m *msg.Transferable) error {
		pubsubCount.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	prod.Produce(ctx, msg.New().SetBody("p2p").SetQueue(p2pQueue))
	prod.Produce(ctx, msg.New().SetBody("pubsub").SetQueue(pubsubQueue))

	time.Sleep(5 * time.Second)

	if p2pCount.Load() != 1 {
		t.Errorf("p2p: %d, want 1", p2pCount.Load())
	}
	if pubsubCount.Load() != 1 {
		t.Errorf("pubsub: %d, want 1", pubsubCount.Load())
	}
}

// Scenario: Pause one queue while others continue consuming
func TestComplex_PauseOneQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	activeQueue := publicqueue.MustQueueParams("test-complex-active")
	pausedQueue := publicqueue.MustQueueParams("test-complex-paused")

	testutil.CreateQueue(t, ctx, activeQueue, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, pausedQueue, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	var activeCount, pausedCount atomic.Int64

	cons := redissmq.NewConsumer()
	cons.Consume(activeQueue, func(ctx context.Context, m *msg.Transferable) error {
		activeCount.Add(1)
		return nil
	})
	cons.Consume(pausedQueue, func(ctx context.Context, m *msg.Transferable) error {
		pausedCount.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	sm := redissmq.NewStateManager()

	// Pause one queue
	sm.Pause(ctx, pausedQueue, nil)

	// Produce to both
	prod.Produce(ctx, msg.New().SetBody("active").SetQueue(activeQueue))
	prod.Produce(ctx, msg.New().SetBody("paused").SetQueue(pausedQueue))

	time.Sleep(3 * time.Second)
	duringPause := pausedCount.Load()
	t.Logf("consumed during pause: active=%d, paused=%d", activeCount.Load(), duringPause)

	if activeCount.Load() == 0 {
		t.Fatal("active queue should have consumed messages")
	}

	// Resume and wait
	sm.Resume(ctx, pausedQueue, nil)
	time.Sleep(5 * time.Second)

	afterResume := pausedCount.Load()
	t.Logf("consumed after resume: active=%d, paused=%d", activeCount.Load(), afterResume)

	if afterResume <= duringPause {
		t.Fatal("paused queue should have consumed messages after resume")
	}
}

// Scenario: High volume of messages across multiple consumers
func TestComplex_HighVolume(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-complex-high-volume")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	messageCount := 100
	consumerCount := 4

	prod := testutil.StartProducer(t, ctx)

	// Produce messages
	for i := 0; i < messageCount; i++ {
		prod.Produce(ctx, msg.New().SetBody(fmt.Sprintf("msg-%d", i)).SetQueue(params))
	}

	// Start multiple consumers
	var totalConsumed atomic.Int64
	var consumers []publicconsumer.Consumer

	for i := 0; i < consumerCount; i++ {
		cons := redissmq.NewConsumer()
		cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
			totalConsumed.Add(1)
			return nil
		})
		if err := cons.Run(ctx); err != nil {
			t.Fatalf("run consumer %d: %v", i, err)
		}
		consumers = append(consumers, cons)
	}

	// Wait for all messages to be consumed
	deadline := time.After(20 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timeout: consumed %d/%d", totalConsumed.Load(), messageCount)
		default:
			if totalConsumed.Load() >= int64(messageCount) {
				goto done
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
done:

	for _, cons := range consumers {
		cons.Shutdown()
	}

	t.Logf("consumed %d/%d messages with %d consumers", totalConsumed.Load(), messageCount, consumerCount)
}

// Scenario: Producer and consumer run concurrently
func TestComplex_ProducerConsumerConcurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-complex-concurrent")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	var produced, consumed atomic.Int64

	// Start consumer
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	// Start producer in goroutine
	prod := testutil.StartProducer(t, ctx)
	go func() {
		for i := 0; i < 50; i++ {
			prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
			produced.Add(1)
		}
	}()

	// Wait for all messages
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timeout: produced=%d, consumed=%d", produced.Load(), consumed.Load())
		default:
			if consumed.Load() >= 50 {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// Scenario: Consumer gracefully shuts down when its context is cancelled.
func TestComplex_GracefulShutdownOnContextCancel(t *testing.T) {
	ctx := testutil.Setup(t)
	consumerCtx, cancel := context.WithCancel(ctx)

	params := publicqueue.MustQueueParams("test-graceful-cancel")
	testutil.CreateQueue(t, consumerCtx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, consumerCtx)
	for i := 0; i < 5; i++ {
		if _, err := prod.Produce(consumerCtx, msg.New().SetBody("msg").SetQueue(params)); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		time.Sleep(100 * time.Millisecond) // simulate work
		return nil
	})
	if err := cons.Run(consumerCtx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}

	// Wait for some messages to be processed.
	time.Sleep(2 * time.Second)
	if consumed.Load() == 0 {
		cancel()
		cons.Shutdown()
		t.Fatal("no messages consumed")
	}

	// Cancel the context and wait for graceful shutdown.
	cancel()
	time.Sleep(3 * time.Second) // allow time for shutdown sequence

	// After graceful shutdown, the consumer should be stopped and its heartbeat key removed.
	if cons.IsRunning() {
		t.Fatal("consumer should not be running after context cancellation")
	}

	heartbeatKey := keys.System{}.ConsumerHeartbeat(cons.ID())
	exists, err := redis.Client().Exists(ctx, heartbeatKey).Result()
	if err != nil {
		t.Fatalf("check heartbeat: %v", err)
	}
	if exists > 0 {
		t.Fatal("heartbeat key should be deleted after graceful shutdown")
	}

	// Verify no messages are left in a stuck state (processing or requeued).
	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	processingLen, _ := redis.Client().LLen(ctx, qKey.ConsumerProcessing(cons.ID())).Result()
	if processingLen > 0 {
		t.Errorf("processing queue should be empty, got %d", processingLen)
	}
	requeuedLen, _ := redis.Client().LLen(ctx, qKey.Requeued()).Result()
	if requeuedLen > 0 {
		t.Errorf("requeued list should be empty, got %d", requeuedLen)
	}

	t.Logf("Consumer gracefully shut down: consumed %d messages, heartbeat deleted, no stuck messages", consumed.Load())
}

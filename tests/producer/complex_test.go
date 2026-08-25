/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Multiple producers on same queue
func TestComplex_MultipleProducers(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-complex-multi-prod")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	producerCount := 5
	messagesPerProducer := 20
	var totalProduced atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < producerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			prod := testutil.StartProducer(t, ctx)
			for j := 0; j < messagesPerProducer; j++ {
				m := msg.New().SetBody("msg").SetQueue(params)
				_, err := prod.Produce(ctx, m)
				if err != nil {
					t.Errorf("producer %d message %d: %v", id, j, err)
					return
				}
				totalProduced.Add(1)
			}
		}(i)
	}

	wg.Wait()

	expected := int64(producerCount * messagesPerProducer)
	if totalProduced.Load() != expected {
		t.Fatalf("produced %d, want %d", totalProduced.Load(), expected)
	}
}

// Scenario: Producer sends to multiple queues
func TestComplex_MultipleQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := publicqueue.MustQueueParams("test-complex-multi-q1")
	q2 := publicqueue.MustQueueParams("test-complex-multi-q2")
	q3 := publicqueue.MustQueueParams("test-complex-multi-q3")

	testutil.CreateQueue(t, ctx, q1, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, publicqueue.TypeLIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q3, publicqueue.TypePriority, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	prod.Produce(ctx, msg.New().SetBody("fifo").SetQueue(q1))
	prod.Produce(ctx, msg.New().SetBody("lifo").SetQueue(q2))
	prod.Produce(ctx, msg.New().SetBody("prio").SetQueue(q3).SetPriority(msg.PriorityHigh))

	//
	qm := redissmq.NewQueueManager()

	// Verify all queues have messages
	props1, _ := qm.Properties(ctx, q1)
	props2, _ := qm.Properties(ctx, q2)
	props3, _ := qm.Properties(ctx, q3)

	if props1.MessagesCount != 1 {
		t.Errorf("q1: %d messages, want 1", props1.MessagesCount)
	}
	if props2.MessagesCount != 1 {
		t.Errorf("q2: %d messages, want 1", props2.MessagesCount)
	}
	if props3.MessagesCount != 1 {
		t.Errorf("q3: %d messages, want 1", props3.MessagesCount)
	}
}

// Scenario: High volume produce
func TestComplex_HighVolume(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-complex-high-volume")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	messageCount := 1000

	start := time.Now()
	for i := 0; i < messageCount; i++ {
		m := msg.New().SetBody("msg").SetQueue(params)
		if _, err := prod.Produce(ctx, m); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	t.Logf("produced %d messages in %v (%.0f msg/s)", messageCount, elapsed, float64(messageCount)/elapsed.Seconds())

	qm := redissmq.NewQueueManager()
	props, _ := qm.Properties(ctx, params)
	if props.MessagesCount != int64(messageCount) {
		t.Errorf("messages = %d, want %d", props.MessagesCount, messageCount)
	}
}

// Scenario: Produce while consumer is consuming
func TestComplex_ProduceWhileConsuming(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-complex-concurrent")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	prod := testutil.StartProducer(t, ctx)
	messageCount := 200

	var wg sync.WaitGroup
	wg.Add(2)

	// Producer goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < messageCount; i++ {
			m := msg.New().SetBody("msg").SetQueue(params)
			if _, err := prod.Produce(ctx, m); err != nil {
				t.Errorf("produce %d: %v", i, err)
				return
			}
		}
	}()

	// Wait for consumption
	go func() {
		defer wg.Done()
		deadline := time.After(15 * time.Second)
		for {
			select {
			case <-deadline:
				return
			default:
				if consumed.Load() >= int64(messageCount) {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	wg.Wait()

	t.Logf("consumed %d/%d messages", consumed.Load(), messageCount)
}

// Scenario: Large number of producers concurrently
func TestComplex_ManyProducers(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-complex-many-prod")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	producerCount := 50
	messagesEach := 10
	var totalProduced atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < producerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			prod := testutil.StartProducer(t, ctx)
			for j := 0; j < messagesEach; j++ {
				m := msg.New().SetBody("msg").SetQueue(params)
				if _, err := prod.Produce(ctx, m); err != nil {
					t.Errorf("produce: %v", err)
					return
				}
				totalProduced.Add(1)
			}
		}()
	}

	wg.Wait()

	expected := int64(producerCount * messagesEach)
	if totalProduced.Load() != expected {
		t.Errorf("produced %d, want %d", totalProduced.Load(), expected)
	}
}

// Scenario: Producer gracefully shuts down when its context is cancelled.
func TestComplex_GracefulShutdownOnContextCancel(t *testing.T) {
	ctx := testutil.Setup(t)
	producerCtx, cancel := context.WithCancel(ctx)

	params := publicqueue.MustQueueParams("test-prod-graceful-cancel")
	testutil.CreateQueue(t, producerCtx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := redissmq.NewProducer()
	if err := prod.Run(producerCtx); err != nil {
		t.Fatalf("run producer: %v", err)
	}

	// Produce a few messages to ensure the producer is working.
	for i := 0; i < 3; i++ {
		m := msg.New().SetBody("msg").SetQueue(params)
		if _, err := prod.Produce(producerCtx, m); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	// Cancel the context.
	cancel()
	time.Sleep(2 * time.Second) // allow time for shutdown sequence

	// After graceful shutdown, the producer should be stopped.
	if prod.IsRunning() {
		t.Fatal("producer should not be running after context cancellation")
	}

	// Verify that attempting to produce after shutdown returns an error.
	_, err := prod.Produce(ctx, msg.New().SetBody("after-cancel").SetQueue(params))
	if err == nil {
		t.Fatal("expected error when producing after shutdown")
	}

	t.Log("Producer gracefully shut down on context cancellation")
}

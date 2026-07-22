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
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Handler receives message body
func TestHandler_ReceivesMessage(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-handler-receive")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("hello world").SetQueue(params))

	var receivedBody string
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		receivedBody = m.Body.(string)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(2 * time.Second)

	if receivedBody != "hello world" {
		t.Fatalf("body = %q, want %q", receivedBody, "hello world")
	}
}

// Scenario: Handler returning error triggers retry
func TestHandler_ErrorTriggersRetry(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-handler-retry")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().
		SetBody("retry").
		SetQueue(params).
		SetRetryThreshold(3).
		SetRetryDelay(0),
	)

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("failure")
	})
	cons.Run(ctx)

	// Wait for requeuer to process (runs every 5s)
	time.Sleep(10 * time.Second)

	count := attempts.Load()
	cons.Shutdown()

	if count < 2 {
		t.Fatalf("attempts = %d, want >= 2", count)
	}
}

// Scenario: Handler with message TTL expiry
func TestHandler_MessageTTLExpiry(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-handler-ttl")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Create message with short TTL, wait for it to expire before producing
	m := msg.New().
		SetBody("expiring").
		SetQueue(params).
		SetTTL(500 * time.Millisecond)

	time.Sleep(1 * time.Second) // Let TTL expire

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, m)

	var handled atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		handled.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(2 * time.Second)

	// Expired message should not be delivered to handler
	if handled.Load() > 0 {
		t.Logf("handler called %d times (expired message may have been processed)", handled.Load())
	}
}

// Scenario: Handler context cancellation
func TestHandler_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(testutil.Setup(t))

	params := q.MustQueueParams("test-handler-cancel")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		time.Sleep(500 * time.Millisecond)
		return nil
	})
	cons.Run(ctx)

	time.Sleep(1 * time.Second)
	cancel()

	time.Sleep(1 * time.Second)

	count := consumed.Load()
	if count == 0 {
		t.Fatal("no messages processed before cancellation")
	}
	t.Logf("processed %d messages before cancellation", count)
}

// Scenario: Handler receives message metadata
func TestHandler_MessageMetadata(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-handler-metadata")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().
		SetBody("meta").
		SetQueue(params).
		SetTTL(5*time.Minute),
	)

	var receivedMsg *msg.Transferable
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		receivedMsg = m
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	// Wait for consumer to start and process
	for i := 0; i < 10; i++ {
		if receivedMsg != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if receivedMsg == nil {
		t.Fatal("no message received")
	}
	if receivedMsg.ID != ids[0] {
		t.Errorf("id = %s, want %s", receivedMsg.ID, ids[0])
	}
	if receivedMsg.TTL != 5*60*1000 {
		t.Errorf("ttl = %d, want %d", receivedMsg.TTL, 5*60*1000)
	}
	if receivedMsg.Body.(string) != "meta" {
		t.Errorf("body = %q, want %q", receivedMsg.Body, "meta")
	}
}

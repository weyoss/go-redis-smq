/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package message_test

import (
	"context"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Delete a single message
func TestDelete_Single(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-delete-single")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().SetBody("delete-me").SetQueue(params))

	result, err := message.Delete(ctx, ids[0])
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if result.Status != msg.DeleteStatusOK {
		t.Errorf("status = %s, want OK", result.Status)
	}
	if result.Stats.Success != 1 {
		t.Errorf("success = %d, want 1", result.Stats.Success)
	}
}

// Scenario: Delete multiple messages
func TestDelete_Multiple(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-delete-multi")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	var ids []string
	for i := 0; i < 5; i++ {
		id, _ := prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
		ids = append(ids, id[0])
	}

	result, err := message.DeleteAll(ctx, ids)
	if err != nil {
		t.Fatalf("delete all: %v", err)
	}
	if result.Status != msg.DeleteStatusOK {
		t.Errorf("status = %s, want OK", result.Status)
	}
	if result.Stats.Success != 5 {
		t.Errorf("success = %d, want 5", result.Stats.Success)
	}
}

// Scenario: Delete non-existent message
func TestDelete_NotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	result, err := message.Delete(ctx, "nonexistent-id")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Non-existent messages are silently skipped
	if result.Stats.NotFound != 1 {
		t.Errorf("notFound = %d, want 1", result.Stats.NotFound)
	}
}

// Scenario: Delete empty list
func TestDelete_EmptyList(t *testing.T) {
	ctx := testutil.Setup(t)

	result, err := message.DeleteAll(ctx, []string{})
	if err != nil {
		t.Fatalf("delete all: %v", err)
	}
	if result.Status != msg.DeleteStatusOK {
		t.Errorf("status = %s, want OK", result.Status)
	}
}

// Scenario: Delete mix of found and not found
func TestDelete_MixedFoundAndNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-delete-mixed")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().SetBody("real").SetQueue(params))

	searchIDs := []string{ids[0], "fake-id-1", "fake-id-2"}

	result, err := message.DeleteAll(ctx, searchIDs)
	if err != nil {
		t.Fatalf("delete all: %v", err)
	}
	if result.Stats.Success != 1 {
		t.Errorf("success = %d, want 1", result.Stats.Success)
	}
	if result.Stats.NotFound != 2 {
		t.Errorf("notFound = %d, want 2", result.Stats.NotFound)
	}
	if result.Stats.Processed != 3 {
		t.Errorf("processed = %d, want 3", result.Stats.Processed)
	}
}

// Scenario: Delete message after it was consumed
func TestDelete_AcknowledgedMessage(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-delete-acked")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().SetBody("ack-me").SetQueue(params))

	// Consume the message to acknowledge it
	received := make(chan struct{})
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		received <- struct{}{}
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	<-received
	time.Sleep(200 * time.Millisecond)

	// Now delete the acknowledged message
	result, err := message.Delete(ctx, ids[0])
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	t.Logf("delete result: status=%s, success=%d, notFound=%d",
		result.Status, result.Stats.Success, result.Stats.NotFound)
}

// Scenario: Delete same message twice
func TestDelete_DoubleDelete(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-delete-double")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().SetBody("delete-twice").SetQueue(params))

	// First delete
	result1, _ := message.Delete(ctx, ids[0])
	// Second delete
	result2, _ := message.Delete(ctx, ids[0])

	t.Logf("first delete: success=%d, notFound=%d", result1.Stats.Success, result1.Stats.NotFound)
	t.Logf("second delete: success=%d, notFound=%d", result2.Stats.Success, result2.Stats.NotFound)

	if result2.Stats.NotFound != 1 {
		t.Errorf("second delete notFound = %d, want 1", result2.Stats.NotFound)
	}
}

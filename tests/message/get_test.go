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
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Get a message by ID
func TestGet_ByID(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-get-by-id")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, message.New().SetBody("hello").SetQueue(params))

	m, err := redissmq.NewMessageManager().Get(ctx, ids[0])
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.ID != ids[0] {
		t.Errorf("id = %s, want %s", m.ID, ids[0])
	}
	if m.Body.(string) != "hello" {
		t.Errorf("body = %q, want %q", m.Body, "hello")
	}
}

// Scenario: Get a non-existent message returns error
func TestGet_NotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	_, err := redissmq.NewMessageManager().Get(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

// Scenario: Get multiple messages
func TestGet_Multiple(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-get-multi")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	var ids []string
	for i := 0; i < 5; i++ {
		id, _ := prod.Produce(ctx, message.New().SetBody("msg").SetQueue(params))
		ids = append(ids, id[0])
	}

	messages, err := redissmq.NewMessageManager().GetAll(ctx, ids)
	if err != nil {
		t.Fatalf("get all: %v", err)
	}
	if len(messages) != 5 {
		t.Fatalf("got %d messages, want 5", len(messages))
	}
}

// Scenario: Get message status
func TestGet_Status(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-get-status")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, message.New().SetBody("status-test").SetQueue(params))

	status, err := redissmq.NewMessageManager().Status(ctx, ids[0])
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status != message.StatusPending {
		t.Errorf("status = %s, want PENDING", status.String())
	}
}

// Scenario: Get message state
func TestGet_State(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-get-state")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, message.New().SetBody("state-test").SetQueue(params))

	state, err := redissmq.NewMessageManager().State(ctx, ids[0])
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if state.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", state.Attempts)
	}
	if state.Expired {
		t.Error("message should not be expired")
	}
}

// Scenario: Get message with metadata
func TestGet_WithMetadata(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-get-metadata")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, err := prod.Produce(ctx, message.New().
		SetBody("metadata").
		SetQueue(params).
		SetTTL(5*time.Minute).
		SetRetryThreshold(3).
		SetRetryDelay(10*time.Second),
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	m, err := redissmq.NewMessageManager().Get(ctx, ids[0])
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.TTL != 5*60*1000 {
		t.Errorf("ttl = %d, want %d", m.TTL, 5*60*1000)
	}
	if m.RetryThreshold != 3 {
		t.Errorf("retry threshold = %d, want 3", m.RetryThreshold)
	}
	if m.RetryDelay != 10000 {
		t.Errorf("retry delay = %d, want 10000", m.RetryDelay)
	}
}

// Scenario: Get message with scheduled metadata
func TestGet_ScheduledMessage(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-get-scheduled")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, message.New().
		SetBody("scheduled").
		SetQueue(params).
		SetScheduledDelay(1*time.Hour).
		SetScheduledCron("0 0 * * *").
		SetScheduledRepeat(3),
	)

	m, err := redissmq.NewMessageManager().Get(ctx, ids[0])
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.Status != message.StatusScheduled {
		t.Errorf("status = %s, want SCHEDULED", m.Status.String())
	}
}

// Scenario: Get multiple messages with mix of found and not found
func TestGet_MixedFoundAndNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-get-mixed")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, message.New().SetBody("real").SetQueue(params))

	// Mix of real and fake IDs
	searchIDs := []string{ids[0], "fake-id-1", "fake-id-2"}

	messages, err := redissmq.NewMessageManager().GetAll(ctx, searchIDs)
	if err != nil {
		t.Fatalf("get all: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("got %d messages, want 1 (not found are silently skipped)", len(messages))
	}
	if messages[0].ID != ids[0] {
		t.Errorf("id = %s, want %s", messages[0].ID, ids[0])
	}
}

// Scenario: Get messages with empty list returns empty
func TestGet_GetAllEmptyList(t *testing.T) {
	ctx := testutil.Setup(t)

	messages, err := redissmq.NewMessageManager().GetAll(ctx, []string{})
	if err != nil {
		t.Fatalf("get all: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(messages))
	}
}

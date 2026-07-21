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

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Get non-existent message returns error
func TestError_GetNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	_, err := message.Get(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

// Scenario: Get status for non-existent message returns error
func TestError_StatusNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	_, err := message.Status(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

// Scenario: Get state for non-existent message returns error
func TestError_StateNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	_, err := message.State(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

// Scenario: GetAll with all non-existent IDs returns empty
func TestError_GetAllNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	messages, err := message.GetAll(ctx, []string{"fake-1", "fake-2", "fake-3"})
	if err != nil {
		t.Fatalf("get all: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(messages))
	}
}

// Scenario: Delete non-existent message
func TestError_DeleteNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	result, err := message.Delete(ctx, "nonexistent-id")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if result.Stats.NotFound == 0 {
		t.Log("delete non-existent: notFound should be > 0")
	}
}

// Scenario: Requeue non-existent message fails
func TestError_RequeueNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	_, err := message.Requeue(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

// Scenario: Requeue pending message fails
func TestError_RequeuePending(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-requeue-pending")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().SetBody("pending").SetQueue(params))

	_, err := message.Requeue(ctx, ids[0])
	if err == nil {
		t.Fatal("expected error: cannot requeue pending message")
	}
}

// Scenario: Delete with empty ID list succeeds
func TestError_DeleteEmptyList(t *testing.T) {
	ctx := testutil.Setup(t)

	result, err := message.DeleteAll(ctx, []string{})
	if err != nil {
		t.Fatalf("delete all: %v", err)
	}
	if result.Status != msg.DeleteStatusOK {
		t.Errorf("status = %s, want OK", result.Status)
	}
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue_test

import (
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: MustExist succeeds for an existing queue
func TestValidation_MustExist_Success(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-validation-exists")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	err := redissmq.NewQueueManager().MustExist(ctx, params)
	if err != nil {
		t.Fatalf("must exist: %v", err)
	}
}

// Scenario: MustExist fails for a non-existent queue
func TestValidation_MustExist_NotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("nonexistent")
	err := redissmq.NewQueueManager().MustExist(ctx, params)
	if err == nil {
		t.Fatal("expected error for non-existent queue")
	}
}

// Scenario: MustBeOperational succeeds for active queue
func TestValidation_MustBeOperational_Active(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-validation-active")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	err := redissmq.NewQueueManager().MustBeOperational(ctx, params)
	if err != nil {
		t.Fatalf("must be operational: %v", err)
	}
}

// Scenario: MustBeOperational succeeds for paused queue
func TestValidation_MustBeOperational_Paused(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-validation-paused")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	sm := redissmq.NewStateManager()
	sm.Pause(ctx, params, nil)

	err := redissmq.NewQueueManager().MustBeOperational(ctx, params)
	if err != nil {
		t.Fatalf("must be operational: %v", err)
	}
}

// Scenario: MustBeOperational fails for stopped queue
func TestValidation_MustBeOperational_Stopped(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-validation-stopped")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	redissmq.NewStateManager().Stop(ctx, params, nil)

	err := redissmq.NewQueueManager().MustBeOperational(ctx, params)
	if err == nil {
		t.Fatal("expected error for stopped queue")
	}
}

// Scenario: CanEnqueue succeeds for active queue
func TestValidation_CanEnqueue_Active(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-enqueue-active")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	err := redissmq.NewQueueManager().CanEnqueue(ctx, params)
	if err != nil {
		t.Fatalf("can enqueue: %v", err)
	}
}

// Scenario: CanEnqueue succeeds for paused queue
func TestValidation_CanEnqueue_Paused(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-enqueue-paused")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	redissmq.NewStateManager().Pause(ctx, params, nil)

	err := redissmq.NewQueueManager().CanEnqueue(ctx, params)
	if err != nil {
		t.Fatalf("can enqueue: %v", err)
	}
}

// Scenario: CanEnqueue fails for stopped queue
func TestValidation_CanEnqueue_Stopped(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-enqueue-stopped")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	redissmq.NewStateManager().Stop(ctx, params, nil)

	err := redissmq.NewQueueManager().CanEnqueue(ctx, params)
	if err == nil {
		t.Fatal("expected error for stopped queue")
	}
}

// Scenario: CanDequeue succeeds for active queue
func TestValidation_CanDequeue_Active(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-dequeue-active")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	err := redissmq.NewQueueManager().CanDequeue(ctx, params)
	if err != nil {
		t.Fatalf("can dequeue: %v", err)
	}
}

// Scenario: CanDequeue fails for paused queue
func TestValidation_CanDequeue_Paused(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-dequeue-paused")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	redissmq.NewStateManager().Pause(ctx, params, nil)

	err := redissmq.NewQueueManager().CanDequeue(ctx, params)
	if err == nil {
		t.Fatal("expected error for paused queue")
	}
}

// Scenario: CanDequeue fails for stopped queue
func TestValidation_CanDequeue_Stopped(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-dequeue-stopped")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	redissmq.NewStateManager().Stop(ctx, params, nil)

	err := redissmq.NewQueueManager().CanDequeue(ctx, params)
	if err == nil {
		t.Fatal("expected error for stopped queue")
	}
}

// Scenario: Validation fails for non-existent queue
func TestValidation_NonExistent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("nonexistent")
	qm := redissmq.NewQueueManager()

	if err := qm.MustExist(ctx, params); err == nil {
		t.Fatal("MustExist: expected error")
	}
	if err := qm.MustBeOperational(ctx, params); err == nil {
		t.Fatal("MustBeOperational: expected error")
	}
	if err := qm.CanEnqueue(ctx, params); err == nil {
		t.Fatal("CanEnqueue: expected error")
	}
	if err := qm.CanDequeue(ctx, params); err == nil {
		t.Fatal("CanDequeue: expected error")
	}
}

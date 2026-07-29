/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package purge_test

import (
	"context"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Enqueue purge with audit disabled returns error
func TestError_EnqueueAuditDisabled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-audit-disabled")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	_, err := queue.PurgeQueue(ctx, params, q.BrowseAcknowledged)
	if err == nil {
		t.Fatal("expected error: audit disabled for acknowledged")
	}

	_, err = queue.PurgeQueue(ctx, params, q.BrowseDeadLettered)
	if err == nil {
		t.Fatal("expected error: audit disabled for dead-lettered")
	}
}

// Scenario: Cancel non-existent job returns error
func TestError_CancelNonExistent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-cancel-notfound")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	err := queue.CancelPurgeJob(ctx, params, "nonexistent-job")
	if err == nil {
		t.Fatal("expected error for non-existent job")
	}
}

// Scenario: Get non-existent job returns error
func TestError_GetNonExistent(t *testing.T) {
	ctx := testutil.Setup(t)

	_, err := queue.GetPurgeJob(ctx, "nonexistent-job")
	if err == nil {
		t.Fatal("expected error for non-existent job")
	}
}

// Scenario: Second purge job fails because queue is locked
func TestError_QueueAlreadyLocked(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-already-locked")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	_, err := queue.PurgeQueue(ctx, params, q.BrowsePending)
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}

	_, err = queue.PurgeQueue(ctx, params, q.BrowsePending)
	if err == nil {
		t.Fatal("expected error: queue is locked")
	}
	t.Logf("second enqueue error (expected): %v", err)
}

// Scenario: Cancel already canceled job
func TestError_CancelAlreadyCanceled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-cancel-twice")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)
	queue.CancelPurgeJob(ctx, params, jobID)

	err := queue.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Logf("second cancel error (may be expected): %v", err)
	}
}

// Scenario: Cancel already completed job
func TestError_CancelAlreadyCompleted(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-error-cancel-completed")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := queue.GetPurgeJob(ctx, jobID)
			if job.Status == q.PurgeJobCompleted {
				err := queue.CancelPurgeJob(ctx, params, jobID)
				if err == nil {
					t.Log("cancel of completed job succeeded (idempotent)")
				} else {
					t.Logf("cancel of completed job failed (expected): %v", err)
				}
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

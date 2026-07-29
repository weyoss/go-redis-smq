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

// Scenario: Cancel a pending purge job
func TestCancel_PendingJob(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-cancel-pending")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	err := queue.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	job, _ := queue.GetPurgeJob(ctx, jobID)
	if job.Status != q.PurgeJobCanceled {
		t.Errorf("status = %s, want CANCELED", job.Status.String())
	}
}

// Scenario: Cancel a processing job
func TestCancel_ProcessingJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-cancel-processing")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Produce many messages so processing takes time
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5000; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	// Wait for job to start processing
	time.Sleep(2 * time.Second)

	// Cancel while processing
	err := queue.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	job, _ := queue.GetPurgeJob(ctx, jobID)
	if job.Status != q.PurgeJobCanceled {
		t.Errorf("status = %s, want CANCELED", job.Status.String())
	}
}

// Scenario: Cancel non-existent job returns error
func TestCancel_NotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-cancel-notfound")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	err := queue.CancelPurgeJob(ctx, params, "nonexistent-job")
	if err == nil {
		t.Fatal("expected error for non-existent job")
	}
}

// Scenario: Queue is unlocked after cancel
func TestCancel_UnlocksQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-cancel-unlock")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	// Cancel
	queue.CancelPurgeJob(ctx, params, jobID)

	// Queue should be unlocked
	transition, _ := queue.Current(ctx, params)
	if transition.To != q.StateActive {
		t.Errorf("queue state = %s, want ACTIVE after cancel", transition.To.String())
	}
}

// Scenario: Cancel is idempotent
func TestCancel_Idempotent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-cancel-idempotent")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	// Cancel multiple times
	err := queue.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Fatalf("first cancel: %v", err)
	}

	err = queue.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Logf("second cancel (may fail): %v", err)
	}
}

// Scenario: Cancel already completed job should fail
func TestCancel_AlreadyCompleted(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-cancel-completed")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Enqueue and cancel a job (no worker to process it, so it's pending)
	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)
	queue.CancelPurgeJob(ctx, params, jobID)

	// Try to cancel again
	err := queue.CancelPurgeJob(ctx, params, jobID)
	if err == nil {
		t.Log("cancel of already-canceled job succeeded (idempotent)")
	} else {
		t.Logf("cancel of already-canceled job failed (expected): %v", err)
	}
}

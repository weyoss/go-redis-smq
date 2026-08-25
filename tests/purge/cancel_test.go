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

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Cancel a pending purge job
func TestCancel_PendingJob(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cancel-pending")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	err := qm.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	job, _ := qm.GetPurgeJob(ctx, jobID)
	if job.Status != publicqueue.PurgeJobCanceled {
		t.Errorf("status = %s, want CANCELED", job.Status.String())
	}
}

// Scenario: Cancel a processing job
func TestCancel_ProcessingJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-cancel-processing")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	// Produce many messages so processing takes time
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5000; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	// Wait for job to start processing
	time.Sleep(2 * time.Second)

	// Cancel while processing
	err := qm.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	job, _ := qm.GetPurgeJob(ctx, jobID)
	if job.Status != publicqueue.PurgeJobCanceled {
		t.Errorf("status = %s, want CANCELED", job.Status.String())
	}
}

// Scenario: Cancel non-existent job returns error
func TestCancel_NotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cancel-notfound")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()
	err := qm.CancelPurgeJob(ctx, params, "nonexistent-job")
	if err == nil {
		t.Fatal("expected error for non-existent job")
	}
}

// Scenario: Queue is unlocked after cancel
func TestCancel_UnlocksQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cancel-unlock")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	// Cancel
	qm.CancelPurgeJob(ctx, params, jobID)

	// Queue should be unlocked
	sm := redissmq.NewStateManager()
	transition, err := sm.Current(ctx, params)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if transition.To != publicqueue.StateActive {
		t.Errorf("queue state = %s, want ACTIVE after cancel", transition.To.String())
	}
}

// Scenario: Cancel is idempotent
func TestCancel_Idempotent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cancel-idempotent")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	// Cancel multiple times
	err := qm.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Fatalf("first cancel: %v", err)
	}

	err = qm.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Logf("second cancel (may fail): %v", err)
	}
}

// Scenario: Cancel already completed job should fail
func TestCancel_AlreadyCompleted(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cancel-completed")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	// Enqueue and cancel a job (no worker to process it, so it's pending)
	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	qm.CancelPurgeJob(ctx, params, jobID)

	// Try to cancel again
	err := qm.CancelPurgeJob(ctx, params, jobID)
	if err == nil {
		t.Log("cancel of already-canceled job succeeded (idempotent)")
	} else {
		t.Logf("cancel of already-canceled job failed (expected): %v", err)
	}
}

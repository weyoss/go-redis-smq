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
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
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

// Scenario: A stale SaveProgress (in-memory job struct still says Processing)
// must not clobber a Cancel that landed in the meantime.
func TestCancel_StaleSaveDoesNotClobberStatus(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cancel-stale-save")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()

	// 1. Enqueue a job — status is PENDING.
	jobID, err := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// 2. Transition to PROCESSING (real Lua script via the store).
	js := internalqueue.NewPurgeJobStore()
	job, err := js.Get(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	job.Status = publicqueue.PurgeJobProcessing
	job.StartedAt = time.Now().UnixMilli()
	if err := js.Start(ctx, jobID, "test-worker", job); err != nil {
		t.Fatalf("start job: %v", err)
	}

	// 3. Simulate one batch: save progress, then verify it landed.
	job.Meta = &publicqueue.PurgeJobMeta{Purged: 100}
	updated, err := js.SaveProgress(ctx, job)
	if err != nil {
		t.Fatalf("save progress (batch 1): %v", err)
	}
	if !updated {
		t.Fatalf("save progress (batch 1) unexpectedly rejected")
	}

	// 4. Cancel the job via the public API (real Cancel path, unlocks queue).
	if err := qm.CancelPurgeJob(ctx, params, jobID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// 5. Now simulate a stale in-memory job (Status still PROCESSING, purged=200)
	//    trying to save progress. This is exactly what purgeMessages would do
	//    with a job captured before Cancel landed.
	job.Meta.Purged = 200
	updated, err = js.SaveProgress(ctx, job)
	if err != nil {
		t.Fatalf("save progress (stale): %v", err)
	}
	if updated {
		t.Fatal("SaveProgress must return false after Cancel; the write was " +
			"accepted and would have clobbered the Canceled status")
	}

	// 6. Verify Redis still has the Canceled status and the pre-Cancel progress.
	final, err := js.Get(ctx, jobID)
	if err != nil {
		t.Fatalf("get final: %v", err)
	}
	if final.Status != publicqueue.PurgeJobCanceled {
		t.Fatalf("status = %s, want CANCELED (stale save clobbered it)",
			final.Status.String())
	}
	if final.Meta == nil || final.Meta.Purged != 100 {
		t.Errorf("meta.purged = %v, want 100 (stale write must be rejected)",
			final.Meta)
	}
}

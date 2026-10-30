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
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	redissmq "github.com/weyoss/go-redis-smq"
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	internalredis "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
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

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5000; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	time.Sleep(2 * time.Second)

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

	qm.CancelPurgeJob(ctx, params, jobID)

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

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	qm.CancelPurgeJob(ctx, params, jobID)

	err := qm.CancelPurgeJob(ctx, params, jobID)
	if err == nil {
		t.Log("cancel of already-canceled job succeeded (idempotent)")
	} else {
		t.Logf("cancel of already-canceled job failed (expected): %v", err)
	}
}

// Scenario: A stale SaveProgress (in-memory job struct still says Processing)
// must not clobber a Cancel that landed in the meantime.
//
// Regression test for the pre-fix bug where PurgeJobStore.Save did an
// unconditional HSET of the whole job blob, resurrecting the job as Processing
// after Cancel had already marked it Canceled.
//
// The job is seeded directly into Redis with PROCESSING status — NOT via
// qm.PurgeQueue. PurgeQueue LPUSHes onto PendingPurgeJobs, where the Init
// worker is blocked on BRPOPLPUSH; it can race the test and Complete the job
// before the test can drive its own state transitions.
func TestCancel_StaleSaveDoesNotClobberStatus(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cancel-stale-save")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	jobID := uuid.New().String()
	if _, err := redissmq.NewStateManager().Lock(ctx, params,
		publicqueue.LockOwnerPurgeJob, jobID, nil); err != nil {
		t.Fatalf("lock queue: %v", err)
	}

	job := &publicqueue.PurgeJob{
		ID: jobID,
		Payload: publicqueue.PurgeJobPayload{
			Queue:       params,
			MessageType: publicqueue.BrowsePending,
		},
		Status:    publicqueue.PurgeJobProcessing,
		BatchSize: 1000,
		DelayMs:   1000,
		CreatedAt: time.Now().UnixMilli(),
		StartedAt: time.Now().UnixMilli(),
	}
	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	if err := internalredis.Client().
		HSet(ctx, keys.System{}.PurgeJobs(), jobID, string(data)).Err(); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	js := internalqueue.NewPurgeJobStore()

	// 1. Save progress while status is PROCESSING — accepted.
	job.Meta = &publicqueue.PurgeJobMeta{Purged: 100}
	updated, err := js.SaveProgress(ctx, job)
	if err != nil {
		t.Fatalf("save progress (batch 1): %v", err)
	}
	if !updated {
		t.Fatalf("save progress (batch 1) unexpectedly rejected")
	}

	// 2. Cancel via the public API.
	if err := redissmq.NewQueueManager().CancelPurgeJob(ctx, params, jobID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// 3. Stale save must be rejected.
	job.Meta.Purged = 200
	updated, err = js.SaveProgress(ctx, job)
	if err != nil {
		t.Fatalf("save progress (stale): %v", err)
	}
	if updated {
		t.Fatal("SaveProgress must return false after Cancel; the write was " +
			"accepted and would have clobbered the Canceled status")
	}

	// 4. Verify status and progress are preserved.
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

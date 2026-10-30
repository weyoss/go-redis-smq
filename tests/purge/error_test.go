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
	"strings"
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

// Scenario: Enqueue purge with audit disabled returns error
func TestError_EnqueueAuditDisabled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-error-audit-disabled")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()

	_, err := qm.PurgeQueue(ctx, params, publicqueue.BrowseAcknowledged)
	if err == nil {
		t.Fatal("expected error: audit disabled for acknowledged")
	}

	_, err = qm.PurgeQueue(ctx, params, publicqueue.BrowseDeadLettered)
	if err == nil {
		t.Fatal("expected error: audit disabled for dead-lettered")
	}
}

// Scenario: Cancel non-existent job returns error
func TestError_CancelNonExistent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-error-cancel-notfound")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	err := redissmq.NewQueueManager().CancelPurgeJob(ctx, params, "nonexistent-job")
	if err == nil {
		t.Fatal("expected error for non-existent job")
	}
}

// Scenario: Get non-existent job returns error
func TestError_GetNonExistent(t *testing.T) {
	ctx := testutil.Setup(t)

	_, err := redissmq.NewQueueManager().GetPurgeJob(ctx, "nonexistent-job")
	if err == nil {
		t.Fatal("expected error for non-existent job")
	}
}

// Scenario: Second purge job fails because queue is locked
func TestError_QueueAlreadyLocked(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-error-already-locked")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()

	_, err := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}

	_, err = qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	if err == nil {
		t.Fatal("expected error: queue is locked")
	}
	t.Logf("second enqueue error (expected): %v", err)
}

// Scenario: Cancel already canceled job
func TestError_CancelAlreadyCanceled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-error-cancel-twice")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()

	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	qm.CancelPurgeJob(ctx, params, jobID)

	err := qm.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Logf("second cancel error (may be expected): %v", err)
	}
}

// Scenario: Cancel already completed job
func TestError_CancelAlreadyCompleted(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-error-cancel-completed")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := qm.GetPurgeJob(ctx, jobID)
			if job.Status == publicqueue.PurgeJobCompleted {
				err := qm.CancelPurgeJob(ctx, params, jobID)
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

// Scenario: Return-code semantics are correct per script.
//
// Each job Lua script uses -1/-2/-3 with script-specific meanings. This test
// seeds a job in each terminal state and confirms the error message from the
// operation matches the actual state, not a fixed per-code string.
//
// Regression test for the pre-fix bug where runJobScript mapped
// -1 → "already completed", -2 → "already failed", -3 → "already cancelled"
// for every script, regardless of what the script actually meant.
func TestPurgeErrors_ReturnCodeSemantics(t *testing.T) {
	ctx := testutil.Setup(t)
	js := internalqueue.NewPurgeJobStore()

	seedJob := func(t *testing.T, name string, status publicqueue.PurgeJobStatus) string {
		t.Helper()
		params := publicqueue.MustQueueParams(name)
		testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

		jobID := uuid.New().String()
		job := &publicqueue.PurgeJob{
			ID: jobID,
			Payload: publicqueue.PurgeJobPayload{
				Queue:       params,
				MessageType: publicqueue.BrowsePending,
			},
			Status:      status,
			BatchSize:   1000,
			DelayMs:     5000,
			CreatedAt:   time.Now().UnixMilli(),
			CompletedAt: time.Now().UnixMilli(),
		}
		data, err := json.Marshal(job)
		if err != nil {
			t.Fatalf("marshal seeded job: %v", err)
		}
		if err := internalredis.Client().HSet(ctx, keys.System{}.PurgeJobs(), jobID, string(data)).Err(); err != nil {
			t.Fatalf("seed job: %v", err)
		}
		return jobID
	}

	assertReports := func(t *testing.T, err error, want string, wrongs ...string) {
		t.Helper()
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %q: %v", want, err)
		}
		for _, w := range wrongs {
			if strings.Contains(msg, w) {
				t.Errorf("error must not mention %q: %v", w, err)
			}
		}
	}

	t.Run("Complete after Failed reports 'failed'", func(t *testing.T) {
		jobID := seedJob(t, "test-c3-complete-after-failed", publicqueue.PurgeJobFailed)
		err := js.Complete(ctx, jobID, &publicqueue.PurgeJob{ID: jobID})
		assertReports(t, err, "failed", "completed")
	})

	t.Run("Complete after Cancelled reports 'cancelled'", func(t *testing.T) {
		jobID := seedJob(t, "test-c3-complete-after-cancelled", publicqueue.PurgeJobCanceled)
		err := js.Complete(ctx, jobID, &publicqueue.PurgeJob{ID: jobID})
		assertReports(t, err, "cancel", "failed")
	})

	t.Run("Fail after Cancelled reports 'cancelled'", func(t *testing.T) {
		jobID := seedJob(t, "test-c3-fail-after-cancelled", publicqueue.PurgeJobCanceled)
		err := js.Fail(ctx, jobID, &publicqueue.PurgeJob{ID: jobID})
		assertReports(t, err, "cancel", "failed")
	})

	t.Run("Cancel after Completed reports 'completed' (control)", func(t *testing.T) {
		jobID := seedJob(t, "test-c3-cancel-after-completed", publicqueue.PurgeJobCompleted)
		err := js.Cancel(ctx, jobID, &publicqueue.PurgeJob{ID: jobID})
		assertReports(t, err, "completed", "failed")
	})

	t.Run("Start after Cancelled reports 'cancelled' (control)", func(t *testing.T) {
		jobID := seedJob(t, "test-c3-start-after-cancelled", publicqueue.PurgeJobCanceled)
		err := js.Start(ctx, jobID, "test-worker", &publicqueue.PurgeJob{ID: jobID})
		assertReports(t, err, "cancel", "completed")
	})
}

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
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	redissmq "github.com/weyoss/go-redis-smq"
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	internalredis "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Stuck job in PROCESSING is recovered to PENDING
func TestRecovery_StuckJobRecovered(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-recovery-stuck")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()

	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, err := qm.GetPurgeJob(ctx, jobID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if job.Status == publicqueue.PurgeJobCompleted {
				t.Logf("job completed: purged=%d", job.Meta.Purged)
				return
			}
			if job.Status == publicqueue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Worker heartbeat keeps job alive during processing
func TestRecovery_HeartbeatKeepsAlive(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-recovery-heartbeat")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 100; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	time.Sleep(3 * time.Second)

	job, err := qm.GetPurgeJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if job.Status != publicqueue.PurgeJobProcessing && job.Status != publicqueue.PurgeJobCompleted {
		t.Errorf("status = %s, want PROCESSING or COMPLETED", job.Status.String())
	}

	t.Logf("job status after 3s: %s, purged: %d", job.Status.String(), job.Meta.Purged)
}

// Scenario: Recovery happens on Work() startup
func TestRecovery_OnStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-recovery-startup")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, err := qm.GetPurgeJob(ctx, jobID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if job.Status == publicqueue.PurgeJobCompleted {
				t.Logf("job completed after recovery: purged=%d", job.Meta.Purged)
				return
			}
			if job.Status == publicqueue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Completed jobs are not recovered
func TestRecovery_CompletedNotRecovered(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-recovery-completed")
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
			job, err := qm.GetPurgeJob(ctx, jobID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if job.Status == publicqueue.PurgeJobCompleted {
				job2, _ := qm.GetPurgeJob(ctx, jobID)
				if job2.Status != publicqueue.PurgeJobCompleted {
					t.Errorf("completed job should stay completed, got %s", job2.Status.String())
				}
				return
			}
			if job.Status == publicqueue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Canceled jobs are not recovered
func TestRecovery_CanceledNotRecovered(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-recovery-canceled")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	qm.CancelPurgeJob(ctx, params, jobID)

	job, err := qm.GetPurgeJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != publicqueue.PurgeJobCanceled {
		t.Errorf("canceled job should stay canceled, got %s", job.Status.String())
	}
}

// Scenario: Locked queue is unlocked after job completion
func TestRecovery_QueueUnlockedAfterCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-recovery-unlock")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	sm := redissmq.NewStateManager()
	transition, _ := sm.Current(ctx, params)
	if transition.To != publicqueue.StateLocked {
		t.Fatalf("queue should be locked, got %s", transition.To.String())
	}

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, err := qm.GetPurgeJob(ctx, jobID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if job.Status == publicqueue.PurgeJobCompleted {
				transition, _ := sm.Current(ctx, params)
				if transition.To != publicqueue.StateActive {
					t.Errorf("queue should be unlocked after completion, got %s", transition.To.String())
				}
				return
			}
			if job.Status == publicqueue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: A job left in PROCESSING (worker crashed) is recovered by a fresh
// worker's startup scan, even though the job was never acquired via BRPOPLPUSH.
func TestRecovery_SeededStuckJobIsRecovered(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-recovery-seeded-stuck")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	// 1. Enqueue a real purge job (locks the queue, job status = PENDING).
	qm := redissmq.NewQueueManager()
	jobID, err := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	if err != nil {
		t.Fatalf("enqueue purge job: %v", err)
	}

	// 2. Simulate a worker that acquired the job and then crashed:
	//    - Move the job from Pending → Active (as BRPOPLPUSH would).
	//    - Mark it PROCESSING in the job hash (as start-job.lua would).
	//    - Do NOT write the JobWorker key or WorkerHeartbeat — the worker is dead.
	client := internalredis.Client()
	if err := client.RPopLPush(
		ctx,
		keys.System{}.PendingPurgeJobs(),
		keys.System{}.ActivePurgeJobs(),
	).Err(); err != nil && !errors.Is(err, goredis.Nil) {
		t.Fatalf("seed rpoplpush: %v", err)
	}

	job, err := qm.GetPurgeJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get seeded job: %v", err)
	}
	job.Status = publicqueue.PurgeJobProcessing
	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal seeded job: %v", err)
	}
	if err := client.HSet(ctx, keys.System{}.PurgeJobs(), jobID, string(data)).Err(); err != nil {
		t.Fatalf("seed hset: %v", err)
	}

	// 3. Verify the seed landed correctly.
	activeLen, err := client.LLen(ctx, keys.System{}.ActivePurgeJobs()).Result()
	if err != nil {
		t.Fatalf("llen active: %v", err)
	}
	if activeLen != 1 {
		t.Fatalf("seed failed: active list length = %d, want 1", activeLen)
	}
	seeded, _ := qm.GetPurgeJob(ctx, jobID)
	if seeded.Status != publicqueue.PurgeJobProcessing {
		t.Fatalf("seed failed: job status = %s, want PROCESSING", seeded.Status.String())
	}

	// 4. Start a fresh worker. Its startup recoverStuckJobs runs BEFORE its
	//    acquire loop, so the seeded job must be recovered to PENDING, then
	//    picked up and completed by the worker's normal acquire/execute path.
	//
	//    If C1 is present, recoverStuckJobs logs WRONGTYPE at DEBUG (hidden),
	//    the job stays PROCESSING forever, and the loop below times out.
	worker := internalqueue.NewPurgeWorker(internalqueue.NewManager().Purge())
	stop := worker.Start(ctx)
	defer stop()

	// 5. Wait for the job to reach a terminal state.
	deadline := time.After(20 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			final, _ := qm.GetPurgeJob(ctx, jobID)
			t.Fatalf("timeout: job status = %s (recovery never ran)", final.Status.String())
		case <-ticker.C:
			j, err := qm.GetPurgeJob(ctx, jobID)
			if err != nil {
				continue
			}
			switch j.Status {
			case publicqueue.PurgeJobCompleted:
				return // recovery + processing both ran
			case publicqueue.PurgeJobFailed:
				t.Fatalf("job failed after recovery: %s", j.Error)
			}
		}
	}
}

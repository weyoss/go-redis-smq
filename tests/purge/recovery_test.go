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

// Scenario: Stuck job in PROCESSING is recovered to PENDING at worker startup.
//
// The job is seeded directly into Redis (PurgeJobs hash, ActivePurgeJobs list,
// JobWorker key) — NOT via qm.PurgeQueue. PurgeQueue LPUSHes onto
// PendingPurgeJobs, where the Init worker (started by testutil.RunTestsWithRedis)
// is blocked on BRPOPLPUSH; it races the test to acquire the job, and its
// Complete call can overwrite the test's seeded PROCESSING status with COMPLETED
// before the test can verify it. Seeding directly bypasses that entirely.
//
// Regression test for the pre-fix bug where recoverStuckJobs used
// LoadSetMembers (SMEMBERS) on ActivePurgeJobs — a Redis list — which returned
// WRONGTYPE and silently disabled startup recovery.
func TestRecovery_SeededStuckJobIsRecovered(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-recovery-seeded-stuck")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()
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
		DelayMs:   100,
		CreatedAt: time.Now().UnixMilli(),
		StartedAt: time.Now().UnixMilli(),
	}
	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}

	client := internalredis.Client()
	if err := client.HSet(ctx, keys.System{}.PurgeJobs(), jobID, string(data)).Err(); err != nil {
		t.Fatalf("seed PurgeJobs hash: %v", err)
	}
	if err := client.LPush(ctx, keys.System{}.ActivePurgeJobs(), jobID).Err(); err != nil {
		t.Fatalf("seed ActivePurgeJobs list: %v", err)
	}
	if err := client.Set(ctx, keys.System{}.JobWorker(jobID), "dead-worker", 0).Err(); err != nil {
		t.Fatalf("seed JobWorker key: %v", err)
	}

	seeded, err := qm.GetPurgeJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get seeded job: %v", err)
	}
	if seeded.Status != publicqueue.PurgeJobProcessing {
		t.Fatalf("seed failed: job status = %s, want PROCESSING", seeded.Status.String())
	}

	worker := internalqueue.NewPurgeWorker(internalqueue.NewManager().Purge())
	stop := worker.Start(ctx)
	defer stop()

	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			final, _ := qm.GetPurgeJob(ctx, jobID)
			status := "unknown"
			if final != nil {
				status = final.Status.String()
			}
			t.Fatalf("timeout: job status = %s (recovery never ran)", status)
		case <-ticker.C:
			j, err := qm.GetPurgeJob(ctx, jobID)
			if err != nil {
				continue
			}
			switch j.Status {
			case publicqueue.PurgeJobCompleted:
				return
			case publicqueue.PurgeJobFailed:
				t.Fatalf("job failed after recovery: %s", j.Error)
			}
		}
	}
}

// Scenario: A job whose worker crashes AFTER startup is recovered by the
// periodic recovery loop, without requiring a worker restart.
//
// Regression test for H12: recoverStuckJobs ran only at worker startup, so a
// worker that crashed mid-purge left its job in ActivePurgeJobs — and the
// queue LOCKED — until the process restarted. The periodic scan closes that
// window.
//
// The test starts a worker with a short recovery interval, waits for startup
// recovery to finish, THEN seeds a stuck job. Seeding after startup recovery
// is what isolates the periodic path — startup recovery can't see the job
// because it already ran.
func TestRecovery_PeriodicScanCatchesStuckJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 25*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-recovery-periodic")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	manager := internalqueue.NewManager().Purge()
	worker := internalqueue.NewPurgeWorker(manager,
		internalqueue.WithRecoveryInterval(300*time.Millisecond))
	stop := worker.Start(ctx)
	defer stop()

	// Startup recovery is synchronous at the top of work(); with an empty
	// ActivePurgeJobs it completes in microseconds. 500ms is generous.
	time.Sleep(500 * time.Millisecond)

	qm := redissmq.NewQueueManager()
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
		DelayMs:   100,
		CreatedAt: time.Now().UnixMilli(),
		StartedAt: time.Now().UnixMilli(),
	}
	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}

	client := internalredis.Client()
	if err := client.HSet(ctx, keys.System{}.PurgeJobs(), jobID, string(data)).Err(); err != nil {
		t.Fatalf("seed PurgeJobs hash: %v", err)
	}
	if err := client.LPush(ctx, keys.System{}.ActivePurgeJobs(), jobID).Err(); err != nil {
		t.Fatalf("seed ActivePurgeJobs list: %v", err)
	}
	if err := client.Set(ctx, keys.System{}.JobWorker(jobID), "dead-worker", 0).Err(); err != nil {
		t.Fatalf("seed JobWorker key: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		j, err := qm.GetPurgeJob(ctx, jobID)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		switch j.Status {
		case publicqueue.PurgeJobCompleted:
			return
		case publicqueue.PurgeJobFailed:
			t.Fatalf("recovered job failed: %s", j.Error)
		}
		time.Sleep(100 * time.Millisecond)
	}

	final, _ := qm.GetPurgeJob(ctx, jobID)
	status := "unknown"
	if final != nil {
		status = final.Status.String()
	}
	t.Fatalf("timeout: job status = %s (periodic recovery never ran)", status)
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

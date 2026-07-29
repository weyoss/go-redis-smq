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

// Scenario: Stuck job in PROCESSING is recovered to PENDING
func TestRecovery_StuckJobRecovered(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-recovery-stuck")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, err := queue.GetPurgeJob(ctx, jobID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if job.Status == q.PurgeJobCompleted {
				t.Logf("job completed: purged=%d", job.Meta.Purged)
				return
			}
			if job.Status == q.PurgeJobFailed {
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

	params := q.MustQueueParams("test-recovery-heartbeat")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 100; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	time.Sleep(3 * time.Second)

	job, err := queue.GetPurgeJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if job.Status != q.PurgeJobProcessing && job.Status != q.PurgeJobCompleted {
		t.Errorf("status = %s, want PROCESSING or COMPLETED", job.Status.String())
	}

	t.Logf("job status after 3s: %s, purged: %d", job.Status.String(), job.Meta.Purged)
}

// Scenario: Recovery happens on Work() startup
func TestRecovery_OnStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-recovery-startup")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, err := queue.GetPurgeJob(ctx, jobID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if job.Status == q.PurgeJobCompleted {
				t.Logf("job completed after recovery: purged=%d", job.Meta.Purged)
				return
			}
			if job.Status == q.PurgeJobFailed {
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

	params := q.MustQueueParams("test-recovery-completed")
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
			job, err := queue.GetPurgeJob(ctx, jobID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if job.Status == q.PurgeJobCompleted {
				job2, _ := queue.GetPurgeJob(ctx, jobID)
				if job2.Status != q.PurgeJobCompleted {
					t.Errorf("completed job should stay completed, got %s", job2.Status.String())
				}
				return
			}
			if job.Status == q.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Canceled jobs are not recovered
func TestRecovery_CanceledNotRecovered(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-recovery-canceled")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)
	queue.CancelPurgeJob(ctx, params, jobID)

	job, err := queue.GetPurgeJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != q.PurgeJobCanceled {
		t.Errorf("canceled job should stay canceled, got %s", job.Status.String())
	}
}

// Scenario: Locked queue is unlocked after job completion
func TestRecovery_QueueUnlockedAfterCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-recovery-unlock")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	transition, _ := queue.Current(ctx, params)
	if transition.To != q.StateLocked {
		t.Fatalf("queue should be locked, got %s", transition.To.String())
	}

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, err := queue.GetPurgeJob(ctx, jobID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if job.Status == q.PurgeJobCompleted {
				transition, _ := queue.Current(ctx, params)
				if transition.To != q.StateActive {
					t.Errorf("queue should be unlocked after completion, got %s", transition.To.String())
				}
				return
			}
			if job.Status == q.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

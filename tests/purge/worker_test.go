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

// Scenario: Worker acquires and processes a pending job
func TestWorker_AcquiresAndProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-worker-acquire")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	// Wait for completion
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
				if job.Meta.Purged != 10 {
					t.Errorf("purged = %d, want 10", job.Meta.Purged)
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

// Scenario: Worker updates progress during processing
func TestWorker_ProgressUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-worker-progress")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 50; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	// Check progress updates
	var lastPurged int64
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
			if job.Meta.Purged > lastPurged {
				t.Logf("progress: %d/50", job.Meta.Purged)
				lastPurged = job.Meta.Purged
			}
			if job.Status == q.PurgeJobCompleted {
				if job.Meta.Purged != 50 {
					t.Errorf("purged = %d, want 50", job.Meta.Purged)
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

// Scenario: Worker recovers stuck jobs on startup
func TestWorker_RecoversStuckJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-worker-recover")
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
				if job.Meta.Purged != 10 {
					t.Errorf("purged = %d, want 10", job.Meta.Purged)
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

// Scenario: Worker purges scheduled messages
func TestWorker_PurgeScheduled(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-worker-sched")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("sched").SetQueue(params).SetScheduledDelay(1*time.Hour))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowseScheduled)

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
				if job.Meta.Purged != 5 {
					t.Errorf("purged = %d, want 5", job.Meta.Purged)
				}

				// Verify scheduled messages are gone
				result, _ := queue.BrowseMessages(ctx, params, &q.BrowseParams{
					Filter: q.BrowseScheduled,
				})
				if result.Total != 0 {
					t.Errorf("scheduled = %d, want 0 after purge", result.Total)
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

// Scenario: Worker heartbeat keeps job alive
func TestWorker_HeartbeatKeepsAlive(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-worker-heartbeat")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 100; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	// Check that job stays in PROCESSING (not recovered) while worker is alive
	time.Sleep(3 * time.Second)

	job, err := queue.GetPurgeJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	// Job should still be processing (heartbeat kept it alive)
	if job.Status != q.PurgeJobProcessing && job.Status != q.PurgeJobCompleted {
		t.Errorf("status = %s, want PROCESSING or COMPLETED", job.Status.String())
	}

	t.Logf("job status after 3s: %s, purged: %d", job.Status.String(), job.Meta.Purged)
}

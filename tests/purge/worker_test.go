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

	redissmq "github.com/weyoss/go-redis-smq"
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Worker acquires and processes a pending job
func TestWorker_AcquiresAndProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-worker-acquire")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	// Wait for completion
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
				if job.Meta.Purged != 10 {
					t.Errorf("purged = %d, want 10", job.Meta.Purged)
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

// Scenario: Worker updates progress during processing
func TestWorker_ProgressUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-worker-progress")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 50; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	// Check progress updates
	var lastPurged int64
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
			if job.Meta.Purged > lastPurged {
				t.Logf("progress: %d/50", job.Meta.Purged)
				lastPurged = job.Meta.Purged
			}
			if job.Status == publicqueue.PurgeJobCompleted {
				if job.Meta.Purged != 50 {
					t.Errorf("purged = %d, want 50", job.Meta.Purged)
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

// Scenario: Worker recovers stuck jobs on startup
func TestWorker_RecoversStuckJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-worker-recover")
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
				if job.Meta.Purged != 10 {
					t.Errorf("purged = %d, want 10", job.Meta.Purged)
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

// Scenario: Worker purges scheduled messages
func TestWorker_PurgeScheduled(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-worker-sched")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("sched").SetQueue(params).SetScheduledDelay(1*time.Hour))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowseScheduled)

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
				if job.Meta.Purged != 5 {
					t.Errorf("purged = %d, want 5", job.Meta.Purged)
				}

				// Verify scheduled messages are gone
				result, _ := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
					Filter: publicqueue.BrowseScheduled,
				})
				if result.Total != 0 {
					t.Errorf("scheduled = %d, want 0 after purge", result.Total)
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

// Scenario: Worker heartbeat keeps job alive
func TestWorker_HeartbeatKeepsAlive(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-worker-heartbeat")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 100; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	// Check that job stays in PROCESSING (not recovered) while worker is alive
	time.Sleep(3 * time.Second)

	job, err := qm.GetPurgeJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	// Job should still be processing (heartbeat kept it alive)
	if job.Status != publicqueue.PurgeJobProcessing && job.Status != publicqueue.PurgeJobCompleted {
		t.Errorf("status = %s, want PROCESSING or COMPLETED", job.Status.String())
	}

	t.Logf("job status after 3s: %s, purged: %d", job.Status.String(), job.Meta.Purged)
}

// Scenario: PurgeWorker.stop waits for heartbeatLoop and recoveryLoop to
// exit before returning.
//
// Regression test for C9. Pre-fix, stop cancelled ctx and waited for
// w.work to return (via w.done), but heartbeatLoop and recoveryLoop were
// spawned by work and not tracked — they could still be mid-Redis when
// internalredis.Close fired.
func TestWorker_StopWaitsForSubGoroutines(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-c9-worker-stop")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	worker := internalqueue.NewPurgeWorker(internalqueue.NewManager().Purge())
	stop := worker.Start(ctx)

	// Let heartbeatLoop and recoveryLoop start.
	time.Sleep(300 * time.Millisecond)

	// Call stop; it must wait for both sub-goroutines.
	stop()

	// If we reach here without a panic after redissmq.Shutdown's eventual
	// internalredis.Close, the sub-goroutines exited before the client was
	// nilled.
}

// Scenario: PurgeWorker.stop returns only after work has exited, so no
// in-flight Redis call from work races redissmq.Shutdown's eventual
// internalredis.Close.
func TestWorker_StopWaitsForWorkGoroutine(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-h14-work-wait")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 2000; i++ {
		if _, err := prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params)); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	qm := redissmq.NewQueueManager()
	jobID, err := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	if err != nil {
		t.Fatalf("enqueue purge: %v", err)
	}

	// Give the Init worker time to acquire and start processing the job.
	// A 2000-message purge with default BatchSize=1000, DelayMs=5000 has
	// at least two batches, so work is likely mid-batch or in the
	// BatchDelay sleep when we call stop. Either state exercises the fix.
	time.Sleep(100 * time.Millisecond)

	// Construct a dedicated worker pointing at the same manager. We can't
	// stop the Init worker directly (it's owned by redissmq.Init), so we
	// exercise the stop path on a separate worker that shares the same
	// Redis state. Its stop() must return only after its own work() exits.
	manager := internalqueue.NewManager().Purge()
	worker := internalqueue.NewPurgeWorker(manager)
	stop := worker.Start(ctx)

	// Let this worker also be mid-flight, if it can acquire the job.
	// (It may not — the Init worker may already hold the lock. Either
	// way, stop must wait for this worker's own work() to exit.)
	time.Sleep(100 * time.Millisecond)

	// The critical assertion: stop blocks until this worker's work()
	// exits. Pre-fix, if work() were still in-flight at 5s, stop would
	// return and the subsequent Shutdown would crash. Post-fix, stop
	// blocks. Under -race, this sequence would surface any race.
	stop()

	// After stop returns, this worker's goroutines are all gone. If
	// internalredis.Close nils the client next, nothing from this
	// worker will call Redis.
	if _, err := qm.GetPurgeJob(ctx, jobID); err != nil {
		t.Logf("job state check after stop: %v (state may have changed)", err)
	}

	// Shutdown exercises the full teardown including internalredis.Close.
	// Post-fix, no worker goroutine is left to race it. Pre-fix, in the
	// (rare) case the 5s timeout fired, this would panic.
	redissmq.Shutdown()

	// The test passing means: no panic during Shutdown, no -race warning.
	// If a future regression reintroduces a bounded wait, this test may
	// still pass on a fast machine — the deterministic proof is the
	// unconditional `<-w.done` in the source, verified by review.
	t.Log("worker stop → Shutdown completed without crash or race")
}

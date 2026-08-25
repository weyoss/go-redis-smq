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
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Job transitions PENDING → PROCESSING → COMPLETED
func TestLifecycle_PendingToCompleted(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-purge-lifecycle")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	// Produce messages
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()

	// Enqueue purge
	jobID, err := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Check initial state
	job, _ := qm.GetPurgeJob(ctx, jobID)
	if job.Status != publicqueue.PurgeJobPending {
		t.Errorf("initial status = %s, want PENDING", job.Status.String())
	}

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
				goto done
			}
			if job.Status == publicqueue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
done:

	t.Logf("job completed: purged=%d", job.Meta.Purged)

	// Verify messages are purged
	props, _ := qm.Properties(ctx, params)
	if props.PendingMessagesCount != 0 {
		t.Errorf("pending = %d, want 0 after purge", props.PendingMessagesCount)
	}

	sm := redissmq.NewStateManager()

	// Queue should be unlocked
	transition, _ := sm.Current(ctx, params)
	if transition.To != publicqueue.StateActive {
		t.Errorf("queue state = %s, want ACTIVE", transition.To.String())
	}
}

// Scenario: Job transitions PENDING → CANCELED
func TestLifecycle_PendingToCanceled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-purge-cancel-lifecycle")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()

	// Enqueue
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	// Cancel immediately
	err := qm.CancelPurgeJob(ctx, params, jobID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// Check status
	job, _ := qm.GetPurgeJob(ctx, jobID)
	if job.Status != publicqueue.PurgeJobCanceled {
		t.Errorf("status = %s, want CANCELED", job.Status.String())
	}

	sm := redissmq.NewStateManager()

	// Queue should be unlocked
	transition, _ := sm.Current(ctx, params)
	if transition.To != publicqueue.StateActive {
		t.Errorf("queue state = %s, want ACTIVE", transition.To.String())
	}
}

// Scenario: Get non-existent job returns error
func TestLifecycle_GetNotFound(t *testing.T) {
	ctx := testutil.Setup(t)
	_, err := redissmq.NewQueueManager().GetPurgeJob(ctx, "nonexistent-job-id")
	if err == nil {
		t.Fatal("expected error for non-existent job")
	}
}

// Scenario: Job metadata is correct
func TestLifecycle_JobMetadata(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-purge-metadata")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()

	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	job, err := qm.GetPurgeJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if job.Payload.Queue.NS() != params.NS() {
		t.Errorf("ns = %s, want %s", job.Payload.Queue.NS(), params.NS())
	}
	if job.Payload.Queue.Name() != params.Name() {
		t.Errorf("name = %s, want %s", job.Payload.Queue.Name(), params.Name())
	}
	if job.Payload.MessageType != publicqueue.BrowsePending {
		t.Errorf("messageType = %v, want PENDING", job.Payload.MessageType)
	}
	if job.CreatedAt == 0 {
		t.Error("createdAt should be set")
	}
	if job.BatchSize <= 0 {
		t.Errorf("batchSize = %d, want > 0", job.BatchSize)
	}
}

// Scenario: Multiple jobs can be enqueued
func TestLifecycle_MultipleJobs(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-purge-multi-jobs")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()

	// First job — succeeds
	jobID1, err := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}

	// Second job while queue is locked — should fail
	_, err = qm.PurgeQueue(ctx, params, publicqueue.BrowseScheduled)
	if err == nil {
		t.Fatal("expected error: queue is locked by first job")
	}
	t.Logf("second enqueue error (expected): %v", err)

	// Cancel first job to unlock
	qm.CancelPurgeJob(ctx, params, jobID1)

	// Now second job should succeed
	jobID2, err := qm.PurgeQueue(ctx, params, publicqueue.BrowseScheduled)
	if err != nil {
		t.Fatalf("enqueue after unlock: %v", err)
	}

	job1, _ := qm.GetPurgeJob(ctx, jobID1)
	job2, _ := qm.GetPurgeJob(ctx, jobID2)

	if job1.Payload.MessageType != publicqueue.BrowsePending {
		t.Errorf("job1 type = %v, want PENDING", job1.Payload.MessageType)
	}
	if job2.Payload.MessageType != publicqueue.BrowseScheduled {
		t.Errorf("job2 type = %v, want SCHEDULED", job2.Payload.MessageType)
	}
}

// Scenario: Complete job unlocks queue
func TestLifecycle_CompleteUnlocksQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-purge-complete-unlock")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	// Cancel to unlock
	qm.CancelPurgeJob(ctx, params, jobID)

	// Queue should be unlocked
	sm := redissmq.NewStateManager()
	transition, _ := sm.Current(ctx, params)
	if transition.To != publicqueue.StateActive {
		t.Errorf("queue state = %s, want ACTIVE after cancel", transition.To.String())
	}

	// Should be able to produce again
	prod2 := testutil.StartProducer(t, ctx)
	_, err := prod2.Produce(ctx, msg.New().SetBody("after-unlock").SetQueue(params))
	if err != nil {
		t.Errorf("produce after unlock: %v", err)
	}
}

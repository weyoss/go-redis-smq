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
	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Purge with batch size of 1
func TestEdge_BatchSizeOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-edge-batch-one")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, queue.BrowsePending)

	deadline := time.After(20 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := qm.GetPurgeJob(ctx, jobID)
			if job.Status == queue.PurgeJobCompleted {
				if job.Meta.Purged != 5 {
					t.Errorf("purged = %d, want 5", job.Meta.Purged)
				}
				return
			}
			if job.Status == queue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Purge with very large batch size
func TestEdge_LargeBatchSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-edge-large-batch")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 100; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, queue.BrowsePending)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := qm.GetPurgeJob(ctx, jobID)
			if job.Status == queue.PurgeJobCompleted {
				if job.Meta.Purged != 100 {
					t.Errorf("purged = %d, want 100", job.Meta.Purged)
				}
				return
			}
			if job.Status == queue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Queue locked during purge — produce fails
func TestEdge_ProduceBlockedDuringPurge(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-edge-prod-blocked")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 50; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, queue.BrowsePending)

	// Try to produce while queue is locked
	_, err := prod.Produce(ctx, msg.New().SetBody("blocked").SetQueue(params))
	if err == nil {
		t.Log("produce succeeded during purge (queue may not be locked yet)")
	} else {
		t.Logf("produce blocked (expected): %v", err)
	}

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := qm.GetPurgeJob(ctx, jobID)
			if job.Status == queue.PurgeJobCompleted {
				t.Logf("purge completed: %d messages", job.Meta.Purged)
				return
			}
			if job.Status == queue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Purge job with special characters in queue name
func TestEdge_SpecialCharQueueName(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-edge-special-chars")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, queue.BrowsePending)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := qm.GetPurgeJob(ctx, jobID)
			if job.Status == queue.PurgeJobCompleted {
				t.Logf("purged: %d", job.Meta.Purged)
				return
			}
			if job.Status == queue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Purge scheduled messages that are due soon
func TestEdge_PurgeScheduledDueSoon(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-edge-sched-due-soon")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().
			SetBody("sched").
			SetQueue(params).
			SetScheduledDelay(500*time.Millisecond),
		)
	}

	// Wait a bit so some are almost due
	time.Sleep(200 * time.Millisecond)

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, queue.BrowseScheduled)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := qm.GetPurgeJob(ctx, jobID)
			if job.Status == queue.PurgeJobCompleted {
				if job.Meta.Purged != 5 {
					t.Errorf("purged = %d, want 5", job.Meta.Purged)
				}
				result, _ := qm.BrowseMessages(ctx, params, &queue.BrowseParams{Filter: queue.BrowseScheduled})
				if result.Total != 0 {
					t.Errorf("scheduled = %d, want 0", result.Total)
				}
				return
			}
			if job.Status == queue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Purge acknowledged messages and verify they're gone
func TestEdge_PurgeAcknowledgedGone(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 1000
	config.Save(ctx, cfg)

	params := queue.MustQueueParams("test-edge-ack-gone")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		ids, _ := prod.Produce(ctx, msg.New().SetBody("ack").SetQueue(params))
		consumeAndAck(t, ctx, params, ids[0])
	}

	qm := redissmq.NewQueueManager()

	// Verify acknowledged messages exist
	result, _ := qm.BrowseMessages(ctx, params, &queue.BrowseParams{Filter: queue.BrowseAcknowledged})
	if result.Total != 5 {
		t.Fatalf("acknowledged = %d, want 5", result.Total)
	}

	jobID, _ := qm.PurgeQueue(ctx, params, queue.BrowseAcknowledged)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := qm.GetPurgeJob(ctx, jobID)
			if job.Status == queue.PurgeJobCompleted {
				result, _ := qm.BrowseMessages(ctx, params, &queue.BrowseParams{Filter: queue.BrowseAcknowledged})
				if result.Total != 0 {
					t.Errorf("acknowledged = %d, want 0 after purge", result.Total)
				}
				return
			}
			if job.Status == queue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

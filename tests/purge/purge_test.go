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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Purge pending messages
func TestPurge_PendingMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-purge-pending")
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
				// Verify no pending messages
				props, _ := queue.Properties(ctx, params)
				if props.PendingMessagesCount != 0 {
					t.Errorf("pending = %d, want 0", props.PendingMessagesCount)
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

// Scenario: Purge scheduled messages
func TestPurge_ScheduledMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-purge-scheduled")
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
				result, _ := queue.BrowseMessages(ctx, params, &q.BrowseParams{Filter: q.BrowseScheduled})
				if result.Total != 0 {
					t.Errorf("scheduled = %d, want 0", result.Total)
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

// Scenario: Purge acknowledged messages with audit enabled
func TestPurge_AcknowledgedMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 1000
	config.Save(ctx, cfg)

	params := q.MustQueueParams("test-purge-ack")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		ids, _ := prod.Produce(ctx, msg.New().SetBody("ack").SetQueue(params))
		consumeAndAck(t, ctx, params, ids[0])
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowseAcknowledged)

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
				return
			}
			if job.Status == q.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Purge dead-lettered messages with audit enabled
func TestPurge_DeadLetteredMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 25*time.Second)
	defer cancel()

	cfg := config.Get()
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.QueueSize = 1000
	config.Save(ctx, cfg)

	params := q.MustQueueParams("test-purge-dlq")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 3; i++ {
		prod.Produce(ctx, msg.New().
			SetBody("dlq").
			SetQueue(params).
			SetRetryThreshold(1).
			SetRetryDelay(0),
		)
	}

	// Fail all messages
	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("fail")
	})
	cons.Run(ctx)
	defer cons.Shutdown()
	time.Sleep(8 * time.Second)

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowseDeadLettered)

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
				t.Logf("purged %d dead-lettered messages", job.Meta.Purged)
				return
			}
			if job.Status == q.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Purge empty queue completes immediately
func TestPurge_EmptyQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-purge-empty")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	deadline := time.After(5 * time.Second)
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
				if job.Meta.Purged != 0 {
					t.Errorf("purged = %d, want 0", job.Meta.Purged)
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

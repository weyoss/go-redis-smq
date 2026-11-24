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
	internalconfig "github.com/weyoss/go-redis-smq/internal/config"
	internalredis "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Purge pending messages
func TestPurge_PendingMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-purge-pending")
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
				// Verify no pending messages
				props, _ := qm.Properties(ctx, params)
				if props.PendingMessagesCount != 0 {
					t.Errorf("pending = %d, want 0", props.PendingMessagesCount)
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

// Scenario: Purge scheduled messages
func TestPurge_ScheduledMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-purge-scheduled")
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
				result, _ := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{Filter: publicqueue.BrowseScheduled})
				if result.Total != 0 {
					t.Errorf("scheduled = %d, want 0", result.Total)
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

// Scenario: Purge acknowledged messages with audit enabled
func TestPurge_AcknowledgedMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	cfg := internalconfig.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 1000
	internalconfig.Save(ctx, cfg)

	params := publicqueue.MustQueueParams("test-purge-ack")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		ids, _ := prod.Produce(ctx, msg.New().SetBody("ack").SetQueue(params))
		consumeAndAck(t, ctx, params, ids[0])
	}

	qm := redissmq.NewQueueManager()

	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowseAcknowledged)

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
				return
			}
			if job.Status == publicqueue.PurgeJobFailed {
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

	cfg := internalconfig.Get()
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.QueueSize = 1000
	internalconfig.Save(ctx, cfg)

	params := publicqueue.MustQueueParams("test-purge-dlq")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

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

	qm := redissmq.NewQueueManager()

	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowseDeadLettered)

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
				t.Logf("purged %d dead-lettered messages", job.Meta.Purged)
				return
			}
			if job.Status == publicqueue.PurgeJobFailed {
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

	params := publicqueue.MustQueueParams("test-purge-empty")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()
	jobID, _ := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)

	deadline := time.After(5 * time.Second)
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
				if job.Meta.Purged != 0 {
					t.Errorf("purged = %d, want 0", job.Meta.Purged)
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

// TestPurge_DoesNotOrphanPendingMessages pins the invariant that a purge
// of Pending either deletes each message or leaves it in Pending —
// never removes the ID without deleting the hash.
//
// Pre-fix, purgeMessages called trimCategory after DeleteMessages. The
// Lua already LREM'd each successfully-deleted ID, so the subsequent
// LTRIM removed the *next* N entries — orphaning them. With N > 2 *
// defaultPurgeBatchSize (1000), roughly half the queue was orphaned per
// run: hashes survived, IDs vanished from the pending list, `purged`
// under-reported.
func TestPurge_DoesNotOrphanPendingMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 60*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-purge-no-orphans")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	const N = 2500 // > 2 * defaultPurgeBatchSize, so at least 3 batches run

	ids := make([]string, 0, N)
	for i := 0; i < N; i++ {
		produced, err := prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		ids = append(ids, produced[0])
	}

	qm := redissmq.NewQueueManager()
	props, err := qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties before purge: %v", err)
	}
	if props.PendingMessagesCount != N {
		t.Fatalf("pending before purge = %d, want %d", props.PendingMessagesCount, N)
	}

	jobID, err := qm.PurgeQueue(ctx, params, publicqueue.BrowsePending)
	if err != nil {
		t.Fatalf("enqueue purge: %v", err)
	}

	deadline := time.After(45 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	var job *publicqueue.PurgeJob
wait:
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for purge completion")
		case <-tick.C:
			j, err := qm.GetPurgeJob(ctx, jobID)
			if err != nil {
				t.Fatalf("get purge job: %v", err)
			}
			switch j.Status {
			case publicqueue.PurgeJobCompleted:
				job = j
				break wait
			case publicqueue.PurgeJobFailed:
				t.Fatalf("purge failed: %s", j.Error)
			}
		}
	}

	// Primary assertion: every message deleted, none orphaned.
	// Buggy code reports purged ≈ 1500 (N/2 orphans); fixed code reports N.
	if job.Meta.Purged != N {
		t.Fatalf("purged = %d, want %d (pre-fix: ~N/2 orphans are silently lost)",
			job.Meta.Purged, N)
	}

	// Defense-in-depth: pending list empty, all hashes gone.
	props, err = qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties after purge: %v", err)
	}
	if props.PendingMessagesCount != 0 {
		t.Errorf("pending after purge = %d, want 0", props.PendingMessagesCount)
	}

	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	pendingIDs, err := internalredis.Client().LRange(ctx, qKey.Pending(), 0, -1).Result()
	if err != nil {
		t.Fatalf("lrange pending: %v", err)
	}
	if len(pendingIDs) != 0 {
		t.Errorf("pending list has %d residual IDs: %v", len(pendingIDs), pendingIDs)
	}

	survivingHashes := 0
	for _, id := range ids {
		exists, err := internalredis.Client().Exists(ctx, keys.System{}.Message(id)).Result()
		if err != nil {
			t.Fatalf("exists message %s: %v", id, err)
		}
		if exists > 0 {
			survivingHashes++
		}
	}
	if survivingHashes > 0 {
		t.Errorf("%d/%d message hashes survived the purge (orphaned)",
			survivingHashes, N)
	}
}

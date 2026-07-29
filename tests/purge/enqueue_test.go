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

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Enqueue purge job for pending messages
func TestEnqueue_PendingMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-purge-enqueue-pending")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Produce some pending messages
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	// Enqueue purge
	jobID, err := queue.PurgeQueue(ctx, params, q.BrowsePending)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if jobID == "" {
		t.Fatal("expected non-empty job ID")
	}
	t.Logf("job ID: %s", jobID)
}

// Scenario: Enqueue purge job for scheduled messages
func TestEnqueue_ScheduledMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-purge-enqueue-sched")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Produce scheduled messages
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 3; i++ {
		prod.Produce(ctx, msg.New().SetBody("sched").SetQueue(params).SetScheduledDelay(1*time.Hour))
	}

	jobID, err := queue.PurgeQueue(ctx, params, q.BrowseScheduled)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if jobID == "" {
		t.Fatal("expected non-empty job ID")
	}
}

// Scenario: Enqueue purge for acknowledged messages requires audit
func TestEnqueue_AcknowledgedMessages_RequiresAudit(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-purge-enqueue-ack-noaudit")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	_, err := queue.PurgeQueue(ctx, params, q.BrowseAcknowledged)
	if err == nil {
		t.Fatal("expected error: audit disabled")
	}
}

// Scenario: Enqueue purge for acknowledged messages with audit enabled
func TestEnqueue_AcknowledgedMessages_WithAudit(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	config.Save(ctx, cfg)

	params := q.MustQueueParams("test-purge-enqueue-ack-audit")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Produce and consume to generate acknowledged messages
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 3; i++ {
		ids, _ := prod.Produce(ctx, msg.New().SetBody("ack").SetQueue(params))
		consumeAndAck(t, ctx, params, ids[0])
	}

	jobID, err := queue.PurgeQueue(ctx, params, q.BrowseAcknowledged)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if jobID == "" {
		t.Fatal("expected non-empty job ID")
	}
}

// Scenario: Enqueue purge for dead-lettered messages requires audit
func TestEnqueue_DeadLetteredMessages_RequiresAudit(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-purge-enqueue-dlq-noaudit")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	_, err := queue.PurgeQueue(ctx, params, q.BrowseDeadLettered)
	if err == nil {
		t.Fatal("expected error: audit disabled")
	}
}

// Scenario: Queue is locked after enqueue
func TestEnqueue_QueueLocked(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-purge-enqueue-locked")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	jobID, err := queue.PurgeQueue(ctx, params, q.BrowsePending)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Check queue state
	transition, err := queue.Current(ctx, params)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if transition.To != q.StateLocked {
		t.Errorf("queue state = %s, want LOCKED", transition.To.String())
	}

	// Try to produce to locked queue — should fail
	prod2 := testutil.StartProducer(t, ctx)
	_, err = prod2.Produce(ctx, msg.New().SetBody("should-fail").SetQueue(params))
	if err == nil {
		t.Error("produce to locked queue should fail")
	}

	t.Logf("job %s locked queue correctly", jobID)
}

// Scenario: Get purge job by ID
func TestEnqueue_GetJob(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-purge-get-job")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	job, err := queue.GetPurgeJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.ID != jobID {
		t.Errorf("job ID = %s, want %s", job.ID, jobID)
	}
	if job.Status != q.PurgeJobPending {
		t.Errorf("status = %s, want PENDING", job.Status.String())
	}
	if job.Payload.MessageType != q.BrowsePending {
		t.Errorf("message type = %v, want PENDING", job.Payload.MessageType)
	}
}

// Helper
func consumeAndAck(t *testing.T, ctx context.Context, params *q.QueueParams, messageID string) {
	t.Helper()
	received := make(chan struct{})
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		if m.ID == messageID {
			received <- struct{}{}
		}
		return nil
	})
	cons.Run(ctx)
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
	cons.Shutdown()
	time.Sleep(200 * time.Millisecond)
}

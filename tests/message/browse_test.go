/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package message_test

import (
	"context"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/config"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Browse published messages
func TestBrowse_PublishedMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-msg-browse-pub")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, publicmessage.New().SetBody("msg").SetQueue(params))
	}

	result, err := redissmq.NewQueueManager().BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePublished,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 5 {
		t.Errorf("total = %d, want 5", result.Total)
	}
}

// Scenario: Browse pending messages
func TestBrowse_PendingMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-msg-browse-pend")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 3; i++ {
		prod.Produce(ctx, publicmessage.New().SetBody("pending").SetQueue(params))
	}

	result, err := redissmq.NewQueueManager().BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePending,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 3 {
		t.Errorf("total = %d, want 3", result.Total)
	}
}

// Scenario: Browse scheduled messages
func TestBrowse_ScheduledMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-msg-browse-sched")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 3; i++ {
		prod.Produce(ctx, publicmessage.New().SetBody("scheduled").SetQueue(params).SetScheduledDelay(1*time.Hour))
	}

	result, err := redissmq.NewQueueManager().BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseScheduled,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 3 {
		t.Errorf("total = %d, want 3", result.Total)
	}
}

// Scenario: Browse acknowledged messages requires audit
func TestBrowse_AcknowledgedMessages_AuditRequired(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-msg-browse-ack-req")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	_, err := redissmq.NewQueueManager().BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseAcknowledged,
	})
	if err == nil {
		t.Fatal("expected error: audit disabled")
	}
}

// Scenario: Browse acknowledged messages with audit enabled
func TestBrowse_AcknowledgedMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 1000
	config.Save(ctx, cfg)

	params := publicqueue.MustQueueParams("test-msg-browse-ack")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 3; i++ {
		prod.Produce(ctx, publicmessage.New().SetBody("ack-browse").SetQueue(params))
	}

	// Consume all messages
	consumed := make(chan struct{}, 3)
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *publicmessage.Transferable) error {
		consumed <- struct{}{}
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	for i := 0; i < 3; i++ {
		<-consumed
	}
	time.Sleep(200 * time.Millisecond)

	result, err := redissmq.NewQueueManager().BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseAcknowledged,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 3 {
		t.Errorf("total = %d, want 3", result.Total)
	}
}

// Scenario: Browse dead-lettered messages requires audit
func TestBrowse_DeadLetteredMessages_AuditRequired(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-msg-browse-dlq-req")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	_, err := redissmq.NewQueueManager().BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseDeadLettered,
	})
	if err == nil {
		t.Fatal("expected error: audit disabled")
	}
}

// Scenario: Browse with pagination
func TestBrowse_Pagination(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-msg-browse-pag")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, publicmessage.New().SetBody("msg").SetQueue(params))
	}

	qm := redissmq.NewQueueManager()

	// Page 1
	result1, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePublished,
		Offset: 0,
		Count:  3,
	})
	if err != nil {
		t.Fatalf("browse page 1: %v", err)
	}
	if len(result1.IDs) != 3 {
		t.Errorf("page 1: %d items, want 3", len(result1.IDs))
	}
	if !result1.HasMore {
		t.Error("page 1 should have more")
	}

	// Page 2
	result2, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePublished,
		Offset: 3,
		Count:  3,
	})
	if err != nil {
		t.Fatalf("browse page 2: %v", err)
	}
	if len(result2.IDs) != 3 {
		t.Errorf("page 2: %d items, want 3", len(result2.IDs))
	}

	// Last page
	result3, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePublished,
		Offset: 9,
		Count:  3,
	})
	if err != nil {
		t.Fatalf("browse page 3: %v", err)
	}
	if len(result3.IDs) != 1 {
		t.Errorf("page 3: %d items, want 1", len(result3.IDs))
	}
	if result3.HasMore {
		t.Error("last page should not have more")
	}
}

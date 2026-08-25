package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Browse published messages
func TestBrowse_PublishedMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-browse-published")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		m := msg.New().SetBody("msg").SetQueue(params)
		if _, err := prod.Produce(ctx, m); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePublished,
		Offset: 0,
		Count:  100,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 10 {
		t.Fatalf("total = %d, want 10", result.Total)
	}
	if len(result.IDs) != 10 {
		t.Fatalf("ids = %d, want 10", len(result.IDs))
	}
	if result.HasMore {
		t.Fatal("HasMore should be false with 10 items and count 100")
	}
}

// Scenario: Browse pending messages (FIFO)
func TestBrowse_PendingMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-browse-pending")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		m := msg.New().SetBody("msg").SetQueue(params)
		prod.Produce(ctx, m)
	}

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePending,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 5 {
		t.Fatalf("total = %d, want 5", result.Total)
	}
}

// Scenario: Browse scheduled messages
func TestBrowse_ScheduledMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-browse-scheduled")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 3; i++ {
		m := msg.New().
			SetBody("scheduled").
			SetQueue(params).
			SetScheduledDelay(1 * time.Hour)
		if _, err := prod.Produce(ctx, m); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseScheduled,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 3 {
		t.Fatalf("total = %d, want 3", result.Total)
	}
}

// Scenario: Browse acknowledged messages requires audit enabled
func TestBrowse_AcknowledgedMessages_AuditDisabled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-browse-ack-disabled")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()
	_, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseAcknowledged,
	})
	if err == nil {
		t.Fatal("expected error when audit is disabled")
	}
}

// Scenario: Browse acknowledged messages with audit enabled
func TestBrowse_AcknowledgedMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	// Enable audit
	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 1000
	if _, err := config.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	params := publicqueue.MustQueueParams("test-browse-ack")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 3; i++ {
		m := msg.New().SetBody("msg").SetQueue(params)
		prod.Produce(ctx, m)
	}

	consumed := make(chan struct{}, 3)
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed <- struct{}{}
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	for i := 0; i < 3; i++ {
		select {
		case <-consumed:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for consumption")
		}
	}

	time.Sleep(200 * time.Millisecond)

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseAcknowledged,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 3 {
		t.Fatalf("total = %d, want 3", result.Total)
	}
}

// Scenario: Browse dead-lettered messages requires audit enabled
func TestBrowse_DeadLetteredMessages_AuditDisabled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-browse-dlq-disabled")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()
	_, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseDeadLettered,
	})
	if err == nil {
		t.Fatal("expected error when audit is disabled")
	}
}

// Scenario: Browse with pagination
func TestBrowse_Pagination(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-browse-pagination")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		m := msg.New().SetBody("msg").SetQueue(params)
		prod.Produce(ctx, m)
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
		t.Fatalf("page 1: %d items, want 3", len(result1.IDs))
	}
	if result1.Total != 10 {
		t.Fatalf("total = %d, want 10", result1.Total)
	}
	if !result1.HasMore {
		t.Fatal("HasMore should be true")
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
		t.Fatalf("page 2: %d items, want 3", len(result2.IDs))
	}

	// Page 3
	result3, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePublished,
		Offset: 6,
		Count:  3,
	})
	if err != nil {
		t.Fatalf("browse page 3: %v", err)
	}
	if len(result3.IDs) != 3 {
		t.Fatalf("page 3: %d items, want 3", len(result3.IDs))
	}

	// Page 4
	result4, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePublished,
		Offset: 9,
		Count:  3,
	})
	if err != nil {
		t.Fatalf("browse page 4: %v", err)
	}
	if len(result4.IDs) != 1 {
		t.Fatalf("page 4: %d items, want 1", len(result4.IDs))
	}
	if result4.HasMore {
		t.Fatal("HasMore should be false on last page")
	}

	// Verify no duplicates across pages
	allIDs := make(map[string]bool)
	for _, r := range []*publicqueue.BrowseResult{result1, result2, result3, result4} {
		for _, id := range r.IDs {
			if allIDs[id] {
				t.Fatalf("duplicate ID: %s", id)
			}
			allIDs[id] = true
		}
	}
	if len(allIDs) != 10 {
		t.Fatalf("unique IDs = %d, want 10", len(allIDs))
	}
}

// Scenario: Browse empty queue
func TestBrowse_EmptyQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-browse-empty")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePublished,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 0 {
		t.Fatalf("total = %d, want 0", result.Total)
	}
	if len(result.IDs) != 0 {
		t.Fatalf("ids = %d, want 0", len(result.IDs))
	}
	if result.HasMore {
		t.Fatal("HasMore should be false for empty queue")
	}
}

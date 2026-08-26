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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	internalconfig "github.com/weyoss/go-redis-smq/internal/config"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Acknowledged queue respects queueSize limit
func TestAudit_AcknowledgedQueueSize(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := internalconfig.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 3
	cfg.MessageAudit.AcknowledgedMessages.Expire = 0
	if _, err := internalconfig.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	params := publicqueue.MustQueueParams("test-audit-ack-queuesize")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Produce and consume 5 messages
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, publicmessage.New().SetBody("msg").SetQueue(params))

		received := make(chan struct{})
		cons := redissmq.NewConsumer()
		cons.Consume(params, func(ctx context.Context, m *publicmessage.Transferable) error {
			received <- struct{}{}
			return nil
		})
		cons.Run(ctx)
		<-received
		cons.Shutdown()
		time.Sleep(100 * time.Millisecond)
	}

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseAcknowledged,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total > 3 {
		t.Errorf("acknowledged total = %d, want <= 3 (queueSize limit)", result.Total)
	}
	t.Logf("acknowledged messages: %d (queueSize: 3)", result.Total)
}

// Scenario: Dead-lettered queue respects queueSize limit
func TestAudit_DeadLetteredQueueSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	cfg := internalconfig.Get()
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.QueueSize = 2
	cfg.MessageAudit.DeadLetteredMessages.Expire = 0
	if _, err := internalconfig.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	params := publicqueue.MustQueueParams("test-audit-dlq-queuesize")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Produce and fail 4 messages (retry threshold 1 = immediate DLQ)
	for i := 0; i < 4; i++ {
		prod.Produce(ctx, publicmessage.New().
			SetBody("dlq").
			SetQueue(params).
			SetRetryThreshold(1).
			SetRetryDelay(0),
		)
	}

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *publicmessage.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("fail")
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(8 * time.Second)

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseDeadLettered,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total > 2 {
		t.Errorf("dead-lettered total = %d, want <= 2 (queueSize limit)", result.Total)
	}
	t.Logf("dead-lettered messages: %d (queueSize: 2)", result.Total)
}

// Scenario: Acknowledged queue respects expire time
func TestAudit_AcknowledgedExpire(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := internalconfig.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 100
	cfg.MessageAudit.AcknowledgedMessages.Expire = 10 // 10 seconds
	if _, err := internalconfig.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	params := publicqueue.MustQueueParams("test-audit-ack-expire")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, publicmessage.New().SetBody("expire-test").SetQueue(params))

	// Consume to acknowledge
	received := make(chan struct{})
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *publicmessage.Transferable) error {
		received <- struct{}{}
		return nil
	})
	cons.Run(ctx)
	<-received
	cons.Shutdown()

	time.Sleep(1 * time.Second)

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseAcknowledged,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 1 {
		t.Fatalf("expected 1 acknowledged message, got %d", result.Total)
	}

	// Wait for expire
	time.Sleep(10 * time.Second)

	result, err = qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseAcknowledged,
	})
	if err != nil {
		t.Fatalf("browse after expire: %v", err)
	}

	if result.Total != 0 {
		t.Fatalf("expected 0 acknowledged message, got %d", result.Total)
	}
}

// Scenario: Dead-lettered queue respects expire time
func TestAudit_DeadLetteredExpire(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := internalconfig.Get()
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.QueueSize = 100
	cfg.MessageAudit.DeadLetteredMessages.Expire = 10 // 10 seconds
	if _, err := internalconfig.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	params := publicqueue.MustQueueParams("test-audit-dlq-expire")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, publicmessage.New().
		SetBody("dlq-expire").
		SetQueue(params).
		SetRetryThreshold(1).
		SetRetryDelay(0),
	)

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *publicmessage.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("fail")
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(1 * time.Second)

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseDeadLettered,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 1 {
		t.Fatalf("expected 1 dead-lettered message, got %d", result.Total)
	}

	// Wait for expire
	time.Sleep(10 * time.Second)

	result, err = qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseDeadLettered,
	})
	if err != nil {
		t.Fatalf("browse after expire: %v", err)
	}

	if result.Total != 0 {
		t.Fatalf("expected 0 dead-lettered message, got %d", result.Total)
	}
}

// Scenario: QueueSize of 0 means unlimited
func TestAudit_UnlimitedQueueSize(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := internalconfig.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 0 // Unlimited
	cfg.MessageAudit.AcknowledgedMessages.Expire = 0
	if _, err := internalconfig.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	params := publicqueue.MustQueueParams("test-audit-unlimited")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Produce and consume 10 messages
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, publicmessage.New().SetBody("msg").SetQueue(params))

		received := make(chan struct{})
		cons := redissmq.NewConsumer()
		cons.Consume(params, func(ctx context.Context, m *publicmessage.Transferable) error {
			received <- struct{}{}
			return nil
		})
		cons.Run(ctx)
		<-received
		cons.Shutdown()
		time.Sleep(100 * time.Millisecond)
	}

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseAcknowledged,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 10 {
		t.Errorf("acknowledged total = %d, want 10 (unlimited)", result.Total)
	}
}

// Scenario: Expire of 0 means never expires
func TestAudit_NeverExpires(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := internalconfig.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 100
	cfg.MessageAudit.AcknowledgedMessages.Expire = 0 // Never
	if _, err := internalconfig.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	params := publicqueue.MustQueueParams("test-audit-never-expire")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, publicmessage.New().SetBody("never-expire").SetQueue(params))

	// Consume
	received := make(chan struct{})
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *publicmessage.Transferable) error {
		received <- struct{}{}
		return nil
	})
	cons.Run(ctx)
	<-received
	cons.Shutdown()
	time.Sleep(100 * time.Millisecond)

	// Wait a bit
	time.Sleep(2 * time.Second)

	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseAcknowledged,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("acknowledged total = %d, want 1 (never expires)", result.Total)
	}
}

// Scenario: Unacknowledgment history with audit enabled
func TestAudit_UnacknowledgmentHistory_WithAudit(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	cfg := internalconfig.Get()
	cfg.MessageAudit.UnacknowledgementHistory.Enabled = true
	cfg.MessageAudit.UnacknowledgementHistory.MaxSize = 100
	if _, err := internalconfig.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	params := publicqueue.MustQueueParams("test-edge-unack-hist-audit")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, publicmessage.New().
		SetBody("unack-audit").
		SetQueue(params).
		SetRetryThreshold(3).
		SetRetryDelay(0),
	)

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *publicmessage.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("fail")
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(5 * time.Second)

	mm := redissmq.NewMessageManager()
	history, err := mm.UnacknowledgmentHistory(ctx, ids[0])
	if err != nil {
		t.Fatalf("unack history: %v", err)
	}
	t.Logf("unacknowledgment history records: %d", len(history))
	if len(history) == 0 {
		t.Error("expected unacknowledgment history records with audit enabled")
	}
}

// Scenario: Unacknowledgment history without audit returns empty
func TestAudit_UnacknowledgmentHistory_NoAudit(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-unack-hist-noaudit")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, publicmessage.New().SetBody("unack-noaudit").SetQueue(params))

	mm := redissmq.NewMessageManager()
	history, err := mm.UnacknowledgmentHistory(ctx, ids[0])
	if err != nil {
		t.Fatalf("unack history: %v", err)
	}
	if len(history) != 0 {
		t.Errorf("expected 0 records without audit, got %d", len(history))
	}
}

// Scenario: Unacknowledgment history respects max size
func TestAudit_UnacknowledgmentHistory_MaxSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	cfg := internalconfig.Get()
	cfg.MessageAudit.UnacknowledgementHistory.Enabled = true
	cfg.MessageAudit.UnacknowledgementHistory.MaxSize = 3
	if _, err := internalconfig.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	params := publicqueue.MustQueueParams("test-edge-unack-maxsize")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, publicmessage.New().
		SetBody("unack-maxsize").
		SetQueue(params).
		SetRetryThreshold(10).
		SetRetryDelay(0),
	)

	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *publicmessage.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("fail")
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(8 * time.Second)

	mm := redissmq.NewMessageManager()
	history, err := mm.UnacknowledgmentHistory(ctx, ids[0])
	if err != nil {
		t.Fatalf("unack history: %v", err)
	}
	t.Logf("attempts: %d, history records: %d (max: 3)", attempts.Load(), len(history))

	if len(history) > 3 {
		t.Errorf("history records = %d, want <= 3 (maxSize)", len(history))
	}
}

// Scenario: Unacknowledgment history for non-existent message
func TestAudit_UnacknowledgmentHistory_NotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	mm := redissmq.NewMessageManager()
	_, err := mm.UnacknowledgmentHistory(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

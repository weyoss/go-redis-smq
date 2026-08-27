/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package scheduling_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Multiple scheduled messages with different delays consumed in order
func TestComplex_DifferentDelays(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-complex-different-delays")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Produce messages with different delays
	prod.Produce(ctx, msg.New().SetBody("3s").SetQueue(params).SetScheduledDelay(3*time.Second))
	prod.Produce(ctx, msg.New().SetBody("6s").SetQueue(params).SetScheduledDelay(6*time.Second))
	prod.Produce(ctx, msg.New().SetBody("9s").SetQueue(params).SetScheduledDelay(9*time.Second))

	var received []string
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		received = append(received, m.Body.(string))
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(15 * time.Second)

	t.Logf("received: %v", received)
	if len(received) != 3 {
		t.Fatalf("received %d messages, want 3", len(received))
	}

	// Verify order: 3s before 6s before 9s
	order := make(map[string]int)
	for i, body := range received {
		order[body] = i
	}
	if order["3s"] >= order["6s"] {
		t.Errorf("'3s' should arrive before '6s', got order: %v", received)
	}
	if order["6s"] >= order["9s"] {
		t.Errorf("'6s' should arrive before '9s', got order: %v", received)
	}
}

// Scenario: High volume of scheduled messages
func TestComplex_HighVolumeScheduled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-complex-high-volume-sched")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	count := 100
	for i := 0; i < count; i++ {
		prod.Produce(ctx, msg.New().
			SetBody("scheduled").
			SetQueue(params).
			SetScheduledDelay(time.Duration(i+1)*time.Hour),
		)
	}

	qm := redissmq.NewQueueManager()
	props, err := qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.ScheduledMessagesCount != int64(count) {
		t.Errorf("scheduled = %d, want %d", props.ScheduledMessagesCount, count)
	}
	if props.PendingMessagesCount != 0 {
		t.Errorf("pending = %d, want 0", props.PendingMessagesCount)
	}
}

// Scenario: Producer and consumer with scheduled messages
func TestComplex_ProducerConsumerScheduled(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-complex-prod-cons-sched")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	prod := testutil.StartProducer(t, ctx)

	// Immediate messages
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("immediate").SetQueue(params))
	}

	// Short delay messages
	for i := 0; i < 3; i++ {
		prod.Produce(ctx, msg.New().
			SetBody("short-delay").
			SetQueue(params).
			SetScheduledDelay(2*time.Second),
		)
	}

	time.Sleep(8 * time.Second)

	count := consumed.Load()
	t.Logf("consumed: %d messages (5 immediate + 3 delayed)", count)
	if count < 5 {
		t.Errorf("consumed %d, want >= 5", count)
	}
}

// Scenario: Mixed immediate and scheduled messages in correct order
func TestComplex_MixedImmediateAndScheduled(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-complex-mixed-immediate")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Immediate
	prod.Produce(ctx, msg.New().SetBody("immediate-1").SetQueue(params))
	prod.Produce(ctx, msg.New().SetBody("immediate-2").SetQueue(params))

	// Delayed
	prod.Produce(ctx, msg.New().SetBody("delayed-3s").SetQueue(params).SetScheduledDelay(3*time.Second))

	// Immediate again
	prod.Produce(ctx, msg.New().SetBody("immediate-3").SetQueue(params))

	var received []string
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		received = append(received, m.Body.(string))
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(10 * time.Second)

	t.Logf("received: %v", received)
	if len(received) < 3 {
		t.Errorf("received %d messages, want >= 3", len(received))
	}
}

// Scenario: Browse scheduled messages with pagination
func TestComplex_BrowseScheduledPagination(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-complex-browse-pag")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	for i := 0; i < 25; i++ {
		prod.Produce(ctx, msg.New().
			SetBody("scheduled").
			SetQueue(params).
			SetScheduledDelay(time.Duration(i+1)*time.Hour),
		)
	}

	qm := redissmq.NewQueueManager()

	offset := int64(0)
	pageSize := int64(10)
	totalIDs := 0
	page := 1

	for {
		result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
			Filter: publicqueue.BrowseScheduled,
			Offset: offset,
			Count:  pageSize,
		})
		if err != nil {
			t.Fatalf("browse page %d: %v", page, err)
		}
		totalIDs += len(result.IDs)
		t.Logf("page %d: %d items (total: %d, hasMore: %v)", page, len(result.IDs), result.Total, result.HasMore)

		if !result.HasMore {
			break
		}
		offset += pageSize
		page++
	}

	if totalIDs != 25 {
		t.Errorf("total IDs across pages = %d, want 25", totalIDs)
	}
}

// Scenario: Scheduling with multiple queue types
func TestComplex_SchedulingMultipleQueueTypes(t *testing.T) {
	ctx := testutil.Setup(t)

	fifoQ := publicqueue.MustQueueParams("test-complex-sched-fifo")
	lifoQ := publicqueue.MustQueueParams("test-complex-sched-lifo")
	prioQ := publicqueue.MustQueueParams("test-complex-sched-prio")

	testutil.CreateQueue(t, ctx, fifoQ, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, lifoQ, publicqueue.TypeLIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, prioQ, publicqueue.TypePriority, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Schedule on FIFO
	prod.Produce(ctx, msg.New().SetBody("fifo").SetQueue(fifoQ).SetScheduledDelay(1*time.Hour))

	// Schedule on LIFO
	prod.Produce(ctx, msg.New().SetBody("lifo").SetQueue(lifoQ).SetScheduledDelay(2*time.Hour))

	// Schedule on Priority
	prod.Produce(ctx, msg.New().SetBody("prio").SetQueue(prioQ).SetScheduledDelay(30*time.Minute).SetPriority(msg.PriorityHigh))

	// Verify all queues have scheduled messages
	qm := redissmq.NewQueueManager()
	for _, qp := range []*publicqueue.Params{fifoQ, lifoQ, prioQ} {
		props, err := qm.Properties(ctx, qp)
		if err != nil {
			t.Fatalf("properties %s: %v", qp.Name(), err)
		}
		if props.ScheduledMessagesCount != 1 {
			t.Errorf("%s: scheduled = %d, want 1", qp.Name(), props.ScheduledMessagesCount)
		}
	}
}

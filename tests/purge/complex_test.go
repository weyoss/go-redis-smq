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
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Full lifecycle — enqueue, process, complete, verify empty
func TestComplex_FullLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-complex-full-lifecycle")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 25; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	props, _ := queue.Properties(ctx, params)
	if props.PendingMessagesCount != 25 {
		t.Fatalf("pending = %d, want 25", props.PendingMessagesCount)
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)
	t.Logf("job created: %s", jobID)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := queue.GetPurgeJob(ctx, jobID)
			if job.Status == q.PurgeJobCompleted {
				if job.Meta.Purged != 25 {
					t.Errorf("purged = %d, want 25", job.Meta.Purged)
				}
				props, _ := queue.Properties(ctx, params)
				if props.PendingMessagesCount != 0 {
					t.Errorf("pending = %d, want 0", props.PendingMessagesCount)
				}
				transition, _ := queue.Current(ctx, params)
				if transition.To != q.StateActive {
					t.Errorf("queue state = %s, want ACTIVE", transition.To.String())
				}
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Multiple jobs on different queues
func TestComplex_MultipleJobsDifferentQueues(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 30*time.Second)
	defer cancel()

	q1 := q.MustQueueParams("test-complex-multi-q1")
	q2 := q.MustQueueParams("test-complex-multi-q2")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("q1").SetQueue(q1))
		prod.Produce(ctx, msg.New().SetBody("q2").SetQueue(q2))
	}

	jobID1, _ := queue.PurgeQueue(ctx, q1, q.BrowsePending)
	jobID2, _ := queue.PurgeQueue(ctx, q2, q.BrowsePending)

	deadline := time.After(20 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timeout: jobs not completed")
		default:
			job1, _ := queue.GetPurgeJob(ctx, jobID1)
			job2, _ := queue.GetPurgeJob(ctx, jobID2)

			if job1.Status == q.PurgeJobCompleted && job2.Status == q.PurgeJobCompleted {
				if job1.Meta.Purged != 10 {
					t.Errorf("job1 purged = %d, want 10", job1.Meta.Purged)
				}
				if job2.Meta.Purged != 10 {
					t.Errorf("job2 purged = %d, want 10", job2.Meta.Purged)
				}
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Purge while messages are being produced
func TestComplex_PurgeWhileProducing(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-complex-purge-while-prod")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 20; i++ {
		prod.Produce(ctx, msg.New().SetBody("initial").SetQueue(params))
	}

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	var produced atomic.Int64
	go func() {
		for i := 0; i < 50; i++ {
			if _, err := prod.Produce(ctx, msg.New().SetBody("during-purge").SetQueue(params)); err != nil {
				break
			}
			produced.Add(1)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := queue.GetPurgeJob(ctx, jobID)
			if job.Status == q.PurgeJobCompleted {
				t.Logf("purged: %d, produced during purge: %d", job.Meta.Purged, produced.Load())
				return
			}
			if job.Status == q.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Purge with consumer active
func TestComplex_PurgeWithConsumerActive(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-complex-purge-with-cons")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 15; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	cons.Run(ctx)

	time.Sleep(1 * time.Second)

	jobID, _ := queue.PurgeQueue(ctx, params, q.BrowsePending)

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job completion")
		default:
			job, _ := queue.GetPurgeJob(ctx, jobID)
			if job.Status == q.PurgeJobCompleted {
				t.Logf("purged: %d, consumed before purge: %d", job.Meta.Purged, consumed.Load())
				return
			}
			if job.Status == q.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Scenario: Rapid enqueue and cancel cycles
func TestComplex_RapidEnqueueCancel(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-complex-rapid-enq-cancel")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	for i := 0; i < 5; i++ {
		jobID, err := queue.PurgeQueue(ctx, params, q.BrowsePending)
		if err != nil {
			t.Fatalf("cycle %d enqueue: %v", i, err)
		}
		err = queue.CancelPurgeJob(ctx, params, jobID)
		if err != nil {
			t.Logf("cycle %d cancel: %v", i, err)
		}
	}
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

package purge_test

import (
	"context"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Purge with a group ID must delete only that group's pending
// messages on a Pub/Sub queue, leaving other groups untouched.
func TestPurgeWithGroup_PubSubPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 20*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-h4-purge-pubsub-pending")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, "group-a"); err != nil {
		t.Fatalf("create group-a: %v", err)
	}
	if _, err := cgm.Save(ctx, params, "group-b"); err != nil {
		t.Fatalf("create group-b: %v", err)
	}

	prod := testutil.StartProducer(t, ctx)
	const messageCount = 3
	for i := 0; i < messageCount; i++ {
		if _, err := prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params)); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	// 3 messages fanned out to 2 groups = 6 total; each group's pending
	// list holds 3.

	qm := redissmq.NewQueueManager()
	parsed := &publicqueue.ParsedParams{
		QueueParams: *params,
		GroupID:     "group-a",
	}
	jobID, err := qm.PurgeQueueWithGroup(ctx, parsed, publicqueue.BrowsePending)
	if err != nil {
		t.Fatalf("enqueue purge: %v", err)
	}

	// Wait for completion.
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for purge job")
		default:
			job, err := qm.GetPurgeJob(ctx, jobID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if job.Status == publicqueue.PurgeJobCompleted {
				if job.Meta.Purged != messageCount {
					t.Errorf("purged = %d, want %d", job.Meta.Purged, messageCount)
				}
				goto verify
			}
			if job.Status == publicqueue.PurgeJobFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
verify:

	// group-a pending should be empty.
	result, err := qm.BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowsePending,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	// Note: BrowseMessages(BrowsePending) reads the queue-level list,
	// which is empty for Pub/Sub regardless. To verify group-specific
	// state we check the job metadata (done above) and the queue's
	// pending count, which is decremented by delete-message.lua.
	_ = result

	props, _ := qm.Properties(ctx, params)
	if props.PendingMessagesCount != messageCount {
		t.Errorf("pending after group-a purge = %d, want %d (group-b only)",
			props.PendingMessagesCount, messageCount)
	}
}

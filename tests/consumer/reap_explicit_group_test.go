/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer_test

import (
	"context"
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: A consumer that crashed while subscribed to an explicit
// consumer group must be removed from that group's member set by the
// reaper. Otherwise ConsumerGroupManager.Delete fails forever with
// CONSUMER_GROUP_HAS_ACTIVE_CONSUMERS, even after every consumer in the
// group is dead — the group is unremovable without manual Redis access.
func TestReaper_RemovesDeadConsumerFromExplicitGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 120*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-c3-explicit-group")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	const groupID = "email-service"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("create group: %v", err)
	}

	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	crashedID := "crashed-consumer-c3"

	// Register the "crashed" consumer. Seed only the state the reaper
	// actually inspects:
	//
	//   - Consumers hash entry: makes HKeys(Consumers) return crashedID,
	//     so the reaper considers it a candidate for recovery.
	//   - ConsumerGroupMembers("email-service"): the C3 leak we're testing
	//     for. This entry must be gone after the reaper runs.
	//   - ConsumerQueues set: mirrors what SubscribeConsumer would have
	//     written; UnsubscribeConsumer cleans it up.
	//
	// Deliberately NOT seeded:
	//   - Heartbeat key — absence is what makes the reaper see it as dead.
	//   - ConsumerProcessing queue — the reaper's unack step is a no-op
	//     when empty, and we're not testing message recovery here.
	//   - Ephemeral group — the recovery path only touches that group if
	//     it exists; here the consumer was in an explicit group, which is
	//     exactly the C3 case.
	if err := redis.Client().HSet(ctx, qKey.Consumers(), crashedID, "{}").Err(); err != nil {
		t.Fatalf("seed consumer hash: %v", err)
	}
	if err := redis.Client().SAdd(ctx, qKey.ConsumerGroupMembers(groupID), crashedID).Err(); err != nil {
		t.Fatalf("seed group membership: %v", err)
	}
	if err := redis.Client().SAdd(ctx, keys.System{}.ConsumerQueues(crashedID), params.String()).Err(); err != nil {
		t.Fatalf("seed consumer queues: %v", err)
	}

	// Sanity: the C3 precondition holds. If this fails, the test setup is
	// wrong and the assertion below would be vacuous.
	isMember, err := redis.Client().SIsMember(ctx, qKey.ConsumerGroupMembers(groupID), crashedID).Result()
	if err != nil {
		t.Fatalf("sismember: %v", err)
	}
	if !isMember {
		t.Fatal("seed failed: crashed consumer not in group")
	}

	// Start a live consumer. Its reaper will find the crashed consumer,
	// recover it, and — post-fix — remove it from the group.
	cons := redissmq.NewConsumer()
	cons.ConsumeWithGroup(params, groupID, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	// Poll for up to 90s — three reaper intervals. The first tick
	// (30s after the consumer's worker lock is acquired) must do the
	// work; the extra 60s is slack for slow CI.
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		isMember, err := redis.Client().SIsMember(ctx, qKey.ConsumerGroupMembers(groupID), crashedID).Result()
		if err != nil {
			t.Fatalf("sismember: %v", err)
		}
		if !isMember {
			// Post-fix: removed. Also confirm the consumer was removed
			// from the Consumers hash — sanity that recoverConsumer ran
			// end-to-end, not just the group-SREM step.
			exists, err := redis.Client().HExists(ctx, qKey.Consumers(), crashedID).Result()
			if err != nil {
				t.Fatalf("hexists consumers: %v", err)
			}
			if exists {
				t.Fatalf("crashed consumer removed from group but still in " +
					"Consumers hash — recoverConsumer bailed early (step 3 " +
					"of UnsubscribeConsumer may have failed)")
			}
			t.Log("crashed consumer recovered: removed from group and Consumers hash")
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Pre-fix: the group membership survives forever. Report the exact
	// state so the failure message is diagnostic.
	groupMembers, _ := redis.Client().SMembers(ctx, qKey.ConsumerGroupMembers(groupID)).Result()
	consumersInHash, _ := redis.Client().HKeys(ctx, qKey.Consumers()).Result()
	t.Fatalf("timeout: crashed consumer still in group after 90s (C3). "+
		"group members=%v, Consumers hash=%v. "+
		"recoverConsumer's explicit-group branch called UnsubscribeConsumer "+
		"with groupID=\"\", so the Lua never ran SREM on ConsumerGroupMembers. "+
		"ConsumerGroupManager.Delete(%q) will fail with "+
		"CONSUMER_GROUP_HAS_ACTIVE_CONSUMERS forever.",
		groupMembers, consumersInHash, groupID,
	)
}

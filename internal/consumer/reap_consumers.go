/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	redisKeys "github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type ReapConsumers struct {
	queue      *queue.Params
	groupID    string
	consumerID string
	interval   time.Duration
	checker    *HeartbeatChecker
	wg         sync.WaitGroup // tracks the reaper ticker goroutine
	log        *slog.Logger
}

func NewReapConsumers(queue *queue.Params, groupID, consumerID string) *ReapConsumers {
	return &ReapConsumers{
		queue:      queue,
		groupID:    groupID,
		consumerID: consumerID,
		interval:   30 * time.Second,
		checker:    NewHeartbeatChecker(),
		log:        logger.New("consumer", "reaper", consumerID, queue.Name()),
	}
}

func (rc *ReapConsumers) Run(ctx context.Context) {
	rc.log.Debug("starting consumer reaper", "interval", rc.interval)
	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				rc.log.Error("consumer reaper panicked", "panic", r)
			}
		}()
		ticker := time.NewTicker(rc.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				rc.log.Debug("consumer reaper stopped")
				return
			case <-ticker.C:
				rc.reap(ctx)
			}
		}
	}()
}

// Wait blocks until the background goroutine exits. Called by
// MessageHandler.beginShutdown after cancel, to guarantee no in-flight
// Redis operation remains when the process tears down the client via
// internalredis.Close.
//
// The goroutine exits as soon as its select observes ctx.Done() — but if
// it's mid-reap (a Redis HKEYS plus MGET heartbeats plus the per-dead-
// consumer recovery work — UnacknowledgeProcessingQueue, UnsubscribeConsumer,
// removeFromAllConsumerGroups), Wait blocks until that entire cycle
// completes. That's the point: shutdown cannot proceed until the last
// Redis op is done.
func (rc *ReapConsumers) Wait() {
	rc.wg.Wait()
}

func (rc *ReapConsumers) reap(ctx context.Context) {
	qKey := redisKeys.Queue{
		Namespace: rc.queue.NS(),
		Name:      rc.queue.Name(),
	}

	consumerIDs, err := redisClient.Client().HKeys(ctx, qKey.Consumers()).Result()
	if err != nil {
		rc.log.Error("failed to load consumer list", "error", err)
		return
	}

	var others []string
	for _, cid := range consumerIDs {
		if cid != rc.consumerID {
			others = append(others, cid)
		}
	}

	if len(others) == 0 {
		return
	}

	rc.log.Debug("checking consumer heartbeats", "total", len(consumerIDs), "others", len(others))

	aliveMap, err := rc.checker.AreAlive(ctx, others)
	if err != nil {
		rc.log.Error("failed to check consumer heartbeats", "error", err)
		return
	}

	deadCount := 0
	for _, cid := range others {
		if aliveMap[cid] {
			continue
		}
		deadCount++
		rc.log.Warn("dead consumer detected — recovering messages",
			"deadConsumerID", cid,
		)
		rc.recoverConsumer(ctx, cid, qKey)
	}

	if deadCount > 0 {
		rc.log.Info("recovered messages from dead consumers", "count", deadCount)
	}
}

// recoverConsumer tears down a dead consumer's state: recovers its in-flight
// messages, deletes its ephemeral group (if any), removes its registration,
// and removes it from every consumer group on the queue.
func (rc *ReapConsumers) recoverConsumer(ctx context.Context, consumerID string, qKey redisKeys.Queue) {
	rc.log.Debug("recovering consumer", "consumerID", consumerID)

	// Step 1: Recover in-flight messages from the dead consumer's processing
	// queue. Must run before unsubscription deletes the ConsumerProcessing
	// key — otherwise the messages would be silently dropped.
	deadUnack := NewMessageUnacknowledger(rc.queue, consumerID)
	if err := deadUnack.UnacknowledgeProcessingQueue(ctx, consumer.CauseOfflineConsumer); err != nil {
		rc.log.Error("failed to unacknowledge messages for dead consumer",
			"consumerID", consumerID,
			"error", err,
		)
	}

	// Step 2: If the dead consumer was using an ephemeral group, delete the
	// group itself. This also removes it from ConsumerGroups and drops its
	// per-group pending lists. Idempotent; skipped if no ephemeral group.
	deadGroupID := ephemeralGroupID(consumerID)
	groupExists, err := redisClient.Client().SIsMember(ctx, qKey.ConsumerGroups(), deadGroupID).Result()
	if err == nil && groupExists {
		if err := DeleteEphemeralConsumerGroup(ctx, consumerID, rc.queue, deadGroupID); err != nil {
			rc.log.Error("failed to delete ephemeral group for dead consumer",
				"consumerID", consumerID,
				"group", deadGroupID,
				"error", err,
			)
		}
	}

	// Step 3: Remove the dead consumer's registration (Consumers hash,
	// ProcessingQueues hash, ConsumerQueues set). Called with groupID=""
	// because the Lua's per-group SREM would be a no-op on the ephemeral
	// group (already deleted in step 2) and doesn't run at all for the
	// explicit-group case. Group cleanup is handled explicitly in step 4.
	if err := UnsubscribeConsumer(ctx, consumerID, rc.queue, ""); err != nil {
		rc.log.Error("failed to unsubscribe dead consumer",
			"consumerID", consumerID,
			"error", err,
		)
	}

	// Step 4 (C3): Unconditionally SREM the dead consumer from every group
	// on this queue. This is the fix. Covers the explicit-group case the
	// Lua misses when groupID="", and is a harmless no-op if the consumer
	// wasn't in any group.
	if err := rc.removeFromAllConsumerGroups(ctx, qKey, consumerID); err != nil {
		rc.log.Error("failed to remove dead consumer from consumer groups",
			"consumerID", consumerID,
			"error", err,
		)
	}

	rc.log.Info("consumer recovered", "consumerID", consumerID)
}

// removeFromAllConsumerGroups removes consumerID from every group's
// ConsumerGroupMembers set on the given queue. Idempotent: SREM on a
// missing member is a no-op. Used by recoverConsumer to close the
// leak where a dead consumer in an explicit group never got removed from
// that group's member set.
func (rc *ReapConsumers) removeFromAllConsumerGroups(ctx context.Context, qKey redisKeys.Queue, consumerID string) error {
	groups, err := redisClient.Client().SMembers(ctx, qKey.ConsumerGroups()).Result()
	if err != nil {
		return fmt.Errorf("load consumer groups: %w", err)
	}
	if len(groups) == 0 {
		return nil
	}

	for _, groupID := range groups {
		membersKey := qKey.ConsumerGroupMembers(groupID)
		if err := redisClient.Client().SRem(ctx, membersKey, consumerID).Err(); err != nil {
			return fmt.Errorf("srem consumer from group %s: %w", groupID, err)
		}
	}
	rc.log.Debug("removed dead consumer from all groups",
		"consumerID", consumerID,
		"groupCount", len(groups),
	)
	return nil
}

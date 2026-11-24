/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	internalConsumer "github.com/weyoss/go-redis-smq/internal/consumer"
	consumerEvents "github.com/weyoss/go-redis-smq/internal/consumer/events"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	redisKeys "github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	publicConsumer "github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// reapConsumers periodically scans the queue's Consumers hash for dead
// consumers (those whose ConsumerHeartbeat key has expired) and recovers
// their state: in-flight messages returned to pending/delayed/dead-letter,
// ephemeral group deleted (if any), registration removed.
//
// Queue-scoped worker: spawned by Coordinator for every queue whose
// WorkersLock this process holds. Runs until ctx is cancelled (by lease
// loss, queue removal, or coordinator shutdown).
//
// Idempotence: every recovery step is safe under concurrent execution
// during the ~10s split-brain window after a lease loss:
//   - UnacknowledgeProcessingQueue is gated by LREM in
//     unacknowledge-message.lua
//   - DeleteEphemeralConsumerGroup's Lua is a no-op if the group is
//     already gone
//   - UnsubscribeConsumer's Lua is a no-op if the consumer is already
//     unregistered
//   - removeFromAllConsumerGroups uses SREM, which is idempotent
//
// Duplicate events during the window are possible but benign — the
// unacknowledged/dead-lettered/requeued/delayed events are informative,
// not authoritative. The underlying message state transitions happen
// exactly once (gated by the LREM).
type reapConsumers struct {
	queue    *queue.Params
	interval time.Duration
	checker  *internalConsumer.HeartbeatChecker
	log      *slog.Logger
}

// newReapConsumers constructs the reaper for the given queue.
//
// No consumer ID: the reaper is not tied to a consumer instance. It
// recovers dead consumers regardless of which consumer elected the
// workers — the old design's per-consumer self-exclusion was a
// partial mitigation for the fresh-consumer race and became
// incorrect once the worker moved to process scope (see heartbeat.go's
// synchronous first beat, which closes the race properly).
func newReapConsumers(qp *queue.Params, interval time.Duration) *reapConsumers {
	return &reapConsumers{
		queue:    qp,
		interval: interval,
		checker:  internalConsumer.NewHeartbeatChecker(),
		log:      logger.New("worker", "reaper", qp.Name()),
	}
}

// Run blocks until ctx is cancelled. Spawned as a goroutine by
// queueWorkers.trySpawnWorkers; the coordinator's per-queue wg tracks
// this Run call, so wg.Wait() waits for the ticker loop (including any
// in-flight reap cycle) to exit.
//
// Recover is local: a panic in reap kills this worker's goroutine but
// does not crash the process. See scheduled_publisher.go for the
// worker-panic / lease-held-until-reconcile caveat.
func (rc *reapConsumers) Run(ctx context.Context) {
	rc.log.Debug("starting consumer reaper", "interval", rc.interval)
	defer rc.log.Debug("consumer reaper stopped")
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
			return
		case <-ticker.C:
			rc.reap(ctx)
		}
	}
}

// runOnce performs a single reap cycle. Test-only entry point —
// production code goes through Run, which wraps reap in a ticker.
//
// Unexported but reachable from internal/worker/*_test.go (same
// package). Used by TestReaper_DoesNotReapFreshConsumer to force a
// reap immediately after a consumer subscribes, before its first
// scheduled heartbeat tick would have fired under the pre-fix code.
func (rc *reapConsumers) runOnce(ctx context.Context) {
	rc.reap(ctx)
}

func (rc *reapConsumers) reap(ctx context.Context) {
	qKey := redisKeys.Queue{
		Namespace: rc.queue.NS(),
		Name:      rc.queue.Name(),
	}

	consumerIDs, err := redisClient.Client().HKeys(ctx, qKey.Consumers()).Result()
	if err != nil {
		rc.log.Error("failed to load consumer list", "error", err)
		return
	}
	if len(consumerIDs) == 0 {
		return
	}

	rc.log.Debug("checking consumer heartbeats", "count", len(consumerIDs))

	aliveMap, err := rc.checker.AreAlive(ctx, consumerIDs)
	if err != nil {
		rc.log.Error("failed to check consumer heartbeats", "error", err)
		return
	}

	deadCount := 0
	for _, cid := range consumerIDs {
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

// recoverConsumer tears down a dead consumer's state: recovers its
// in-flight messages, deletes its ephemeral group (if any), removes its
// registration, and removes it from every consumer group on the queue.
func (rc *reapConsumers) recoverConsumer(ctx context.Context, consumerID string, qKey redisKeys.Queue) {
	rc.log.Debug("recovering consumer", "consumerID", consumerID)

	// Determine which group the dead consumer belonged to, if any. Used
	// only for event payloads — the recovery actions themselves are
	// group-agnostic (the ephemeral group is derived from consumerID,
	// and group membership cleanup is done separately below).
	//
	// The old code used rc.groupID here — the reaper's OWN group, which
	// was wrong. It's meaningless at process scope (no single "the"
	// reaper group exists) and was wrong even at consumer scope (the
	// reaper's group is not necessarily the dead consumer's group).
	deadGroupID := rc.findConsumerGroup(ctx, qKey, consumerID)

	// Step 1: Recover in-flight messages from the dead consumer's
	// processing queue. Must run before unsubscription deletes the
	// ConsumerProcessing key — otherwise the messages would be silently
	// dropped.
	deadUnack := internalConsumer.NewMessageUnacknowledger(rc.queue, consumerID)
	recovered, err := deadUnack.UnacknowledgeProcessingQueue(ctx, publicConsumer.CauseOfflineConsumer)
	if err != nil {
		rc.log.Error("failed to unacknowledge messages for dead consumer",
			"consumerID", consumerID,
			"error", err,
		)
	}

	// Publish the event pair (unacknowledged + action-specific) for each
	// message the Lua actually transitioned. The payload's ConsumerID is
	// the dead consumer's ID — the messages belonged to its processing
	// queue. Subscribers distinguish these events from live unacks by
	// the Cause field (CauseOfflineConsumer).
	//
	// Skipped entries (Processed == false) are silent by design: the Lua
	// skipped them because they were already gone from ConsumerProcessing
	// (peer recovery, concurrent batch). Firing an event for them would
	// misreport the transition as having happened here.
	queueParsed := queue.ParsedParams{
		QueueParams: *rc.queue,
		GroupID:     deadGroupID,
	}
	for _, rec := range recovered {
		if !rec.Result.Processed {
			rc.log.Debug("skipping event for already-recovered message",
				"messageID", rec.Message.ID(),
				"consumerID", consumerID,
			)
			continue
		}

		msgID := rec.Message.ID()
		consumerEvents.PublishMessageUnacknowledged(
			ctx, msgID, queueParsed, consumerID, int(publicConsumer.CauseOfflineConsumer),
		)

		switch rec.Result.Action {
		case internalConsumer.ActionDeadLetter:
			consumerEvents.PublishMessageDeadLettered(
				ctx, msgID, queueParsed, consumerID, int(rec.Result.Cause),
			)
		case internalConsumer.ActionRequeue:
			consumerEvents.PublishMessageRequeued(ctx, msgID, queueParsed, consumerID)
		case internalConsumer.ActionDelay:
			consumerEvents.PublishMessageDelayed(ctx, msgID, queueParsed, consumerID)
		}
	}

	// Step 2: If the dead consumer was using an ephemeral group, delete
	// it. Safe to call unconditionally — DeleteEphemeralConsumerGroup
	// derives the ephemeral group ID from consumerID and the Lua is a
	// no-op for a non-existent group. An explicit (shared) group is
	// untouched; step 4 removes the dead consumer from it.
	if err := internalConsumer.DeleteEphemeralConsumerGroup(ctx, consumerID, rc.queue, ""); err != nil {
		rc.log.Error("failed to delete ephemeral group for dead consumer",
			"consumerID", consumerID,
			"error", err,
		)
	}

	// Step 3: Remove the dead consumer's registration (Consumers hash,
	// ProcessingQueues hash, ConsumerQueues set). Called with groupID=""
	// because the Lua's per-group SREM would be a no-op on the ephemeral
	// group (already deleted in step 2) and doesn't run at all for the
	// explicit-group case. Group cleanup is handled explicitly in step 4.
	if err := internalConsumer.UnsubscribeConsumer(ctx, consumerID, rc.queue, ""); err != nil {
		rc.log.Error("failed to unsubscribe dead consumer",
			"consumerID", consumerID,
			"error", err,
		)
	}

	// Step 4: Unconditionally SREM the dead consumer from every group
	// on this queue. Covers the explicit-group case the Lua misses when
	// groupID="" (step 3), and is a harmless no-op if the consumer
	// wasn't in any group.
	if err := rc.removeFromAllConsumerGroups(ctx, qKey, consumerID); err != nil {
		rc.log.Error("failed to remove dead consumer from consumer groups",
			"consumerID", consumerID,
			"error", err,
		)
	}

	rc.log.Info("consumer recovered", "consumerID", consumerID, "group", deadGroupID)
}

// findConsumerGroup returns the consumer group the given consumer
// belongs to on this queue, or "" for point-to-point subscriptions
// and for consumers not found in any group's member set.
//
// A consumer is a member of at most one group per queue — enforced by
// subscribe-consumer.lua, which adds the consumer to at most one
// ConsumerGroupMembers(groupID) set. If multiple groups contain the
// consumer (invariant violation, shouldn't happen), returns the first
// match and logs a warning.
func (rc *reapConsumers) findConsumerGroup(
	ctx context.Context,
	qKey redisKeys.Queue,
	consumerID string,
) string {
	groups, err := redisClient.Client().SMembers(ctx, qKey.ConsumerGroups()).Result()
	if err != nil {
		rc.log.Debug("failed to list consumer groups", "error", err)
		return ""
	}
	if len(groups) == 0 {
		return ""
	}

	var found []string
	for _, g := range groups {
		member, err := redisClient.Client().
			SIsMember(ctx, qKey.ConsumerGroupMembers(g), consumerID).Result()
		if err == nil && member {
			found = append(found, g)
		}
	}

	switch len(found) {
	case 0:
		return ""
	case 1:
		return found[0]
	default:
		rc.log.Warn("consumer belongs to multiple groups (invariant violation)",
			"consumerID", consumerID,
			"groups", found,
		)
		return found[0]
	}
}

// removeFromAllConsumerGroups removes consumerID from every group's
// ConsumerGroupMembers set on the given queue. Idempotent: SREM on a
// missing member is a no-op. Used by recoverConsumer to close the
// leak where a dead consumer in an explicit group never got removed
// from that group's member set.
func (rc *reapConsumers) removeFromAllConsumerGroups(ctx context.Context, qKey redisKeys.Queue, consumerID string) error {
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

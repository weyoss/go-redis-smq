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
	"log/slog"
	"time"

	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	redisKeys "github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type ReapConsumers struct {
	queue      *queue.Params
	groupID    string
	consumerID string
	interval   time.Duration
	checker    *HeartbeatChecker
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
	go func() {
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
		rc.recoverConsumer(ctx, cid)
	}

	if deadCount > 0 {
		rc.log.Info("recovered messages from dead consumers", "count", deadCount)
	}
}

func (rc *ReapConsumers) recoverConsumer(ctx context.Context, consumerID string) {
	rc.log.Debug("recovering consumer",
		"consumerID", consumerID,
	)

	// Use the dead consumer's ID to create an unacknowledger for its processing queue.
	deadUnack := NewMessageUnacknowledger(rc.queue, consumerID)
	if err := deadUnack.UnacknowledgeProcessingQueue(ctx, CauseOfflineConsumer); err != nil {
		rc.log.Error("failed to unacknowledge messages for dead consumer",
			"consumerID", consumerID,
			"error", err,
		)
	}

	if err := DeleteEphemeralConsumerGroup(ctx, consumerID, rc.queue, rc.groupID); err != nil {
		rc.log.Error("failed to delete ephemeral group for dead consumer",
			"consumerID", consumerID,
			"error", err,
		)
	}

	if err := UnsubscribeConsumer(ctx, consumerID, rc.queue, rc.groupID); err != nil {
		rc.log.Error("failed to unsubscribe dead consumer",
			"consumerID", consumerID,
			"error", err,
		)
	}

	rc.log.Info("consumer recovered",
		"consumerID", consumerID,
	)
}

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
	"time"

	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

type OrphanedLockRecoverer struct {
	queue      *q.QueueParams
	consumerID string
	interval   time.Duration
	log        *slog.Logger
}

func NewOrphanedLockRecoverer(queue *q.QueueParams, consumerID string) *OrphanedLockRecoverer {
	return &OrphanedLockRecoverer{
		queue:      queue,
		consumerID: consumerID,
		interval:   30 * time.Second,
		log:        logger.New("consumer", "lock-recoverer", consumerID, queue.Name()),
	}
}

func (olr *OrphanedLockRecoverer) Run(ctx context.Context) {
	olr.log.Debug("starting orphaned lock recoverer", "interval", olr.interval)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				olr.log.Error("orphaned lock recoverer panicked", "panic", r)
			}
		}()
		ticker := time.NewTicker(olr.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				olr.log.Debug("orphaned lock recoverer stopped")
				return
			case <-ticker.C:
				olr.recover(ctx)
			}
		}
	}()
}

func (olr *OrphanedLockRecoverer) recover(ctx context.Context) {
	qKey := keys.Queue{
		Namespace: olr.queue.NS(),
		Name:      olr.queue.Name(),
	}

	stateStr, err := redisClient.LoadHashField(ctx, qKey.Properties(),
		qSchema.QueueFieldOperationalState.Key(), "queue state")
	if err != nil {
		olr.log.Debug("failed to load queue state", "error", err)
		return
	}

	var state int
	fmt.Sscanf(stateStr, "%d", &state)

	if q.QueueState(state) != q.StateLocked {
		return
	}

	lockID, err := redisClient.LoadHashField(ctx, qKey.Properties(),
		qSchema.QueueFieldLockID.Key(), "lock ID")
	if err != nil || lockID == "" {
		olr.log.Debug("failed to load lock ID", "error", err)
		return
	}

	olr.log.Debug("found locked queue", "lockID", lockID)

	if !olr.isPurgeJobDone(ctx, lockID) {
		olr.log.Debug("purge job still active — skipping unlock", "lockID", lockID)
		return
	}

	olr.log.Info("purge job completed — unlocking queue", "lockID", lockID)
	olr.unlockQueue(ctx, qKey, lockID)
}

func (olr *OrphanedLockRecoverer) isPurgeJobDone(ctx context.Context, jobID string) bool {
	jobKey := keys.System{}.JobWorker(jobID)
	exists, err := redisClient.Client().Exists(ctx, jobKey).Result()
	if err != nil {
		olr.log.Debug("failed to check job worker", "jobID", jobID, "error", err)
		return false
	}
	return exists == 0
}

func (olr *OrphanedLockRecoverer) unlockQueue(ctx context.Context, qKey keys.Queue, lockID string) {
	luaKeys := []string{qKey.Properties(), qKey.StateHistory()}
	argv := []interface{}{
		qSchema.QueueFieldOperationalState.Key(),
		q.StateActive.Int(),
		"",
		q.StateLocked.Int(),
		q.StateActive.Int(),
		100,
		q.StateLocked.Int(),
		lockID,
		qSchema.QueueFieldLastStateChangeAt.Key(),
		time.Now().UnixMilli(),
		qSchema.QueueFieldLockID.Key(),
	}

	_, err := redisClient.Eval(ctx, scripts.SetQueueState, luaKeys, argv...)
	if err != nil {
		olr.log.Error("failed to unlock queue", "lockID", lockID, "error", err)
	} else {
		olr.log.Info("queue unlocked successfully",
			"queue", fmt.Sprintf("%s/%s", olr.queue.NS(), olr.queue.Name()),
			"lockID", lockID,
		)
	}
}

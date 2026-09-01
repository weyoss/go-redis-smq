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
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type OrphanedLockRecoverer struct {
	queue      *queue.Params
	consumerID string
	interval   time.Duration
	log        *slog.Logger
}

func NewOrphanedLockRecoverer(q *queue.Params, consumerID string) *OrphanedLockRecoverer {
	return &OrphanedLockRecoverer{
		queue:      q,
		consumerID: consumerID,
		interval:   30 * time.Second,
		log:        logger.New("consumer", "lock-recoverer", consumerID, q.Name()),
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

	state, err := strconv.Atoi(stateStr)
	if err != nil {
		olr.log.Error("invalid queue state value",
			"value", stateStr,
			"error", err,
		)
		return
	}

	if queue.State(state) != queue.StateLocked {
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
	now := time.Now().UnixMilli()
	from := queue.StateLocked
	transition := queue.StateTransition{
		From:      &from,
		To:        queue.StateActive,
		Reason:    queue.TransitionReason(queue.ReasonPurgeComplete),
		Timestamp: now,
		LockID:    &lockID,
	}
	transitionJSON, err := json.Marshal(transition)
	if err != nil {
		olr.log.Error("failed to marshal unlock transition", "error", err)
		return
	}

	luaKeys := []string{qKey.Properties(), qKey.StateHistory()}
	argv := []interface{}{
		qSchema.QueueFieldOperationalState.Key(),
		queue.StateActive.Int(),
		string(transitionJSON),
		queue.StateLocked.Int(), // expected previous state
		queue.StateActive.Int(), // active state value
		100,                     // max history size
		queue.StateLocked.Int(), // locked state value
		lockID,
		qSchema.QueueFieldLastStateChangeAt.Key(),
		strconv.FormatInt(now, 10),
		qSchema.QueueFieldLockID.Key(),
	}

	_, err = redisClient.Eval(ctx, scripts.SetQueueState, luaKeys, argv...)
	if err != nil {
		olr.log.Error("failed to unlock queue", "lockID", lockID, "error", err)
	} else {
		olr.log.Info("queue unlocked successfully",
			"queue", fmt.Sprintf("%s/%s", olr.queue.NS(), olr.queue.Name()),
			"lockID", lockID,
		)
	}
}

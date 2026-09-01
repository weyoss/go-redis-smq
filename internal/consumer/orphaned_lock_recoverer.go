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
	"log/slog"
	"strconv"
	"time"

	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
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

// OrphanedLockRecovererOption is a functional option for OrphanedLockRecoverer.
type OrphanedLockRecovererOption func(*OrphanedLockRecoverer)

// WithOrphanedLockRecoverInterval sets the interval between recovery checks.
func WithOrphanedLockRecoverInterval(d time.Duration) OrphanedLockRecovererOption {
	return func(o *OrphanedLockRecoverer) {
		if d > 0 {
			o.interval = d
		}
	}
}

// NewOrphanedLockRecoverer creates a new OrphanedLockRecoverer.
func NewOrphanedLockRecoverer(
	q *queue.Params,
	consumerID string,
	opts ...OrphanedLockRecovererOption,
) *OrphanedLockRecoverer {
	r := &OrphanedLockRecoverer{
		queue:      q,
		consumerID: consumerID,
		interval:   30 * time.Second,
		log:        logger.New("consumer", "lock-recoverer", consumerID, q.Name()),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
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

	// Load current state.
	state, err := internalqueue.NewState().FetchCurrent(ctx, olr.queue)
	if err != nil {
		olr.log.Debug("failed to load queue state", "error", err)
		return
	}
	if state.To != queue.StateLocked {
		return
	}

	// Check lock owner: only recover locks owned by the purge job.
	if state.Owner == nil || *state.Owner != queue.LockOwnerPurgeJob {
		olr.log.Debug("lock owner is not purge job; skipping", "owner", state.Owner)
		return
	}

	lockID := ""
	if state.LockID != nil {
		lockID = *state.LockID
	}
	if lockID == "" {
		olr.log.Debug("locked queue has empty lock ID; skipping")
		return
	}

	olr.log.Debug("found locked queue", "lockID", lockID)

	// Check if the worker associated with the job is still alive.
	if internalqueue.IsWorkerAlive(ctx, lockID) {
		olr.log.Debug("purge worker still alive — skipping unlock", "lockID", lockID)
		return
	}

	olr.log.Info("purge worker is dead — unlocking queue", "lockID", lockID)
	olr.unlockQueue(ctx, qKey, lockID)
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
		strconv.Itoa(queue.StateActive.Int()),
		string(transitionJSON),
		strconv.Itoa(queue.StateLocked.Int()), // expected previous state
		strconv.Itoa(queue.StateActive.Int()), // active state value
		"100",                                 // max history size
		strconv.Itoa(queue.StateLocked.Int()), // locked state value
		lockID,
		qSchema.QueueFieldLastStateChangeAt.Key(),
		strconv.FormatInt(now, 10),
		qSchema.QueueFieldLockID.Key(),
	}

	reply, err := redisClient.Eval(ctx, scripts.SetQueueState, luaKeys, argv...)
	if err != nil {
		olr.log.Error("failed to unlock queue", "lockID", lockID, "error", err)
		return
	}
	if replyStr, ok := reply.(string); ok && replyStr != "OK" {
		olr.log.Error("unlock queue script returned error", "reply", replyStr)
		return
	}
	olr.log.Info("queue unlocked successfully", "lockID", lockID)
}

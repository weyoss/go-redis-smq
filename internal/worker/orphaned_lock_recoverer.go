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

// orphanedLockRecoverer periodically checks for queues left in the
// Locked state by a purge worker that has since died, and unlocks them.
//
// A queue is locked at purge start (state.go's acquireLock) with
// LockOwnerPurgeJob and a lock ID equal to the purge job ID. The purge
// job records its worker's liveness via WorkerHeartbeat(workerID). If
// that process crashes, the job is left PROCESSING forever and the
// queue stays LOCKED indefinitely — blocking all consumes and produces
// for that queue. This worker detects the dead-worker condition and
// unlocks the queue.
//
// Queue-scoped worker: spawned by Coordinator for every queue whose
// WorkersLock this process holds. Runs until ctx is cancelled (by lease
// loss, queue removal, or coordinator shutdown).
//
// Idempotence: set-queue-state.lua's INVALID_STATE_TRANSITION check
// (expectedPreviousState) gates concurrent unlock attempts. Only one
// executor transitions Locked → Active; others get a non-OK reply and
// log Debug. Safe under concurrent execution during the ~10s
// split-brain window after a lease loss.
type orphanedLockRecoverer struct {
	queue    *queue.Params
	interval time.Duration
	log      *slog.Logger
}

// newOrphanedLockRecoverer constructs the recoverer for the given queue.
//
// No consumer ID: the recoverer is not tied to a consumer instance. It
// unlocks queues whose purge worker is dead, regardless of which process
// elected the workers.
//
// No option pattern: the interval is fixed at 30s (matching the reaper
// and the coordinator's reconcile cadence). The original exported a
// WithOrphanedLockRecoverInterval option, but it existed only for a
// test that doesn't exist in the current suite. If a future test needs
// a faster tick, re-introduce the option then — don't carry dead
// API surface speculatively.
func newOrphanedLockRecoverer(qp *queue.Params, interval time.Duration) *orphanedLockRecoverer {
	return &orphanedLockRecoverer{
		queue:    qp,
		interval: interval,
		log:      logger.New("worker", "lock-recoverer", qp.Name()),
	}
}

// Run blocks until ctx is cancelled. Spawned as a goroutine by
// queueWorkers.trySpawnWorkers; the coordinator's per-queue wg tracks
// this Run call, so wg.Wait() waits for the ticker loop (including any
// in-flight recover) to exit.
//
// Recover is local: a panic in recover kills this worker's goroutine
// but does not crash the process. See scheduled_publisher.go for the
// worker-panic / lease-held-until-reconcile caveat.
func (olr *orphanedLockRecoverer) Run(ctx context.Context) {
	olr.log.Debug("starting orphaned lock recoverer", "interval", olr.interval)
	defer olr.log.Debug("orphaned lock recoverer stopped")
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
			return
		case <-ticker.C:
			olr.recover(ctx)
		}
	}
}

func (olr *orphanedLockRecoverer) recover(ctx context.Context) {
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

func (olr *orphanedLockRecoverer) unlockQueue(ctx context.Context, qKey keys.Queue, lockID string) {
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
		qSchema.OperationalState.Key(),
		strconv.Itoa(queue.StateActive.Int()),
		string(transitionJSON),
		strconv.Itoa(queue.StateLocked.Int()), // expected previous state
		strconv.Itoa(queue.StateActive.Int()), // active state value
		"100",                                 // max history size
		strconv.Itoa(queue.StateLocked.Int()), // locked state value
		lockID,
		qSchema.LastStateChangeAt.Key(),
		strconv.FormatInt(now, 10),
		qSchema.LockID.Key(),
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

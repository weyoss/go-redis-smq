/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	rdb "github.com/redis/go-redis/v9"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
)

// PurgeWorker is a background worker that processes purge jobs.
//
// It uses its own dedicated Redis client to avoid blocking the shared pool
// during long-running batch operations. The worker acquires jobs from the
// pending list, hands them to the PurgeManager for execution, and maintains
// a heartbeat so other workers can detect a crash.
type PurgeWorker struct {
	manager *PurgeManager

	client *rdb.Client

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	started bool
	mu      sync.Mutex
	log     *slog.Logger
}

// NewPurgeWorker creates a new purge worker for the given manager.
func NewPurgeWorker(manager *PurgeManager) *PurgeWorker {
	return &PurgeWorker{
		manager: manager,
		log:     logger.New("purge", "worker", manager.workerID),
	}
}

// Start launches the purge worker and returns a stop function.
//
// The stop function is safe to call multiple times and will wait for the
// worker goroutine to exit. A dedicated Redis client is created when the
// worker starts and closed when it stops.
func (w *PurgeWorker) Start(ctx context.Context) func() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.started {
		return w.stop
	}

	opts := redisClient.Client().Options()
	w.client = rdb.NewClient(opts)
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.done = make(chan struct{})

	go func() {
		defer close(w.done)
		w.work(w.ctx)
	}()

	w.started = true

	return w.stop
}

// stop terminates the purge worker and waits for it to exit.
func (w *PurgeWorker) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.started {
		return
	}

	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}

	if w.done != nil {
		select {
		case <-w.done:
		case <-time.After(5 * time.Second):
		}
		w.done = nil
	}

	if w.client != nil {
		_ = w.client.Close()
		w.client = nil
	}

	w.started = false
}

// work is the main loop. It recovers stuck jobs, starts the heartbeat, and
// then continuously acquires and executes pending purge jobs.
func (w *PurgeWorker) work(ctx context.Context) {
	// Recover jobs left in processing state by a previous worker that died.
	w.manager.recoverStuckJobs(ctx)

	// Heartbeat keeps the worker alive in Redis.
	go w.heartbeatLoop(ctx)

	w.log.Info("purge worker started")

	for {
		select {
		case <-ctx.Done():
			w.log.Info("purge worker stopped")
			return
		default:
		}

		jobID, err := acquire(ctx, w.client)
		if err != nil {
			w.log.Debug("acquire job failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if jobID == "" {
			continue
		}

		w.manager.execute(ctx, jobID)
	}
}

// heartbeatLoop periodically writes a heartbeat key so that other workers
// can detect this worker is alive.
func (w *PurgeWorker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(workerHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			key := keys.System{}.WorkerHeartbeat(w.manager.workerID)
			if err := redisClient.Client().Set(ctx, key, "1", workerHeartbeatTTL).Err(); err != nil {
				w.log.Error("heartbeat failed", "error", err)
			}
		}
	}
}

// acquire atomically moves a job from the pending list to the active list.
func acquire(ctx context.Context, client *rdb.Client) (string, error) {
	val, err := client.BRPopLPush(
		ctx,
		keys.System{}.PendingPurgeJobs(),
		keys.System{}.ActivePurgeJobs(),
		popTimeout,
	).Result()

	if errors.Is(err, rdb.Nil) {
		return "", nil
	}
	return val, err
}

// IsWorkerAlive reports whether the worker associated with a purge job is
// still alive (has a heartbeat key).
func IsWorkerAlive(ctx context.Context, jobID string) bool {
	workerID, err := redisClient.Client().Get(ctx, keys.System{}.JobWorker(jobID)).Result()
	if err != nil || workerID == "" {
		return false
	}
	heartbeatKey := keys.System{}.WorkerHeartbeat(workerID)
	exists, err := redisClient.Client().Exists(ctx, heartbeatKey).Result()
	if err != nil {
		return false
	}
	return exists > 0
}

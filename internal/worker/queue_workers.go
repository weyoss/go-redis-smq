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
	"sync"

	"github.com/weyoss/go-redis-smq/internal/util/lock"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// queueWorkers holds the state for one queue whose WorkersLock this
// coordinator instance currently holds.
//
// Lifecycle:
//   - Created by Coordinator.trySpawnWorkers once the lease is acquired.
//   - Inserted into Coordinator.active BEFORE the worker goroutines start,
//     so onLost and reconcile can always find it.
//   - Workers run until cancel() fires: onLost (lease stolen), reconcile
//     (queue removed), or Coordinator.stop (shutdown).
//   - Teardown is qw.stop(), which cancels, waits, and releases. Idempotent.
//
// The five worker instances themselves are locals in trySpawnWorkers —
// nothing outside that function calls methods on them. They observe
// workerCtx via their Run(ctx) and exit when it's cancelled.
type queueWorkers struct {
	// queue identifies the queue these workers serve. Used for logging
	// and as the key into Coordinator.active (via queue.String()).
	// Never mutated after construction.
	queue *queue.Params

	// lock is the per-queue WorkersLock lease. Its refresh loop calls
	// cancel() (via WithOnLost) if the lease is stolen. IsAcquired()
	// is the authoritative signal reconcile uses to detect stale
	// entries: false means the lease was lost and qw should be
	// torn down.
	lock *lock.Lock

	// cancel cancels the per-queue context passed to every worker's
	// Run(ctx). Safe to call multiple times. Invoked by:
	//   - Coordinator.stop (shutdown)
	//   - reconcileOnce (queue removed or lease stale)
	//   - the WithOnLost callback (lease stolen)
	//   - qw.stop (which composes cancel + wait + release)
	// All callers tolerate concurrent invocation.
	cancel context.CancelFunc

	// wg tracks the five worker goroutines. wg.Add(5) is called in
	// trySpawnWorkers BEFORE the go statements, so Wait is safe to call
	// even if the goroutines haven't started yet.
	wg sync.WaitGroup
}

// String returns the queue identifier for logging. Matches
// queue.Params.String() (name@namespace).
func (qw *queueWorkers) String() string {
	if qw == nil || qw.queue == nil {
		return ""
	}
	return qw.queue.String()
}

// isStale reports whether the lease backing these workers has been lost.
//
// A stale entry means: the WithOnLost callback has fired (the lease's
// refresh loop observed ExtendLock returning 0, indicating another
// process acquired the key after TTL expiry). workerCtx is already
// cancelled — workers are exiting or exited — and reconcile should
// remove this entry from Coordinator.active and wait for cleanup.
//
// A non-stale entry with a live lease may still be in the middle of its
// worker tick, which is fine — workers are supposed to run.
func (qw *queueWorkers) isStale() bool {
	return qw == nil || qw.lock == nil || !qw.lock.IsAcquired()
}

// stop cancels the worker set, waits for all five goroutines to exit,
// and releases the lease.
//
// Idempotent: qw.cancel is safe to call multiple times; WaitGroup.Wait
// is safe for concurrent callers on an already-draining group; and
// lock.Release returns nil without touching Redis if the lease was
// already released (including the lost-lease case, where onLost cleared
// the acquired flag before the callback fired).
//
// Returns the Release error, if any. Callers log it — qw has no logger
// because it has no identity beyond its queue, and the coordinator
// already logs with that context.
func (qw *queueWorkers) stop() error {
	qw.cancel()
	qw.wg.Wait()
	return qw.lock.Release(context.Background())
}

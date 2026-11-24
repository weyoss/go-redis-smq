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

	"github.com/google/uuid"
	redissmq "github.com/weyoss/go-redis-smq"
	internalredis "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// The recoverer runs inside the process-scoped Coordinator, spawned by
// redissmq.Init (which TestMain calls). It is not constructed directly
// by these tests and has no public API to invoke it.
//
// Observation path:
//
//	1. Test creates a queue and locks it (simulating a crashed purge
//	   job whose worker never wrote a heartbeat).
//	2. Coordinator's reconcile loop (every 30s) discovers the new queue,
//	   acquires its WorkersLock lease, spawns the five per-queue workers.
//	3. The recoverer's own 30s ticker fires, checks the queue state,
//	   sees the purge owner without a live worker, and unlocks.
//	4. Test's polling loop sees the transition to Active.
//
// Timing. Worst case is ~60s: up to 30s for the coordinator's next
// reconcile tick (the new queue wasn't present at the previous tick),
// then up to 30s for the recoverer's first tick. A 120s deadline gives
// 2x slack.

// orphanedLockRecovererDeadline is scaled to the LockRecoverer cadence.
// With TestIntervals (500ms) this is 10s — twenty ticks, ample slack for
// even the slowest CI. With production intervals it caps at 2 minutes.
var orphanedLockRecovererDeadline = testutil.IntervalDeadline(
	testutil.WorkerIntervals().LockRecoverer,
)

// TestOrphanedLockRecoverer_UnlocksWhenWorkerNeverStarted covers the
// primary path: a queue locked by a purge job whose worker never wrote
// its JobWorker mapping. The recoverer's IsWorkerAlive(jobID) returns
// false, and the queue is unlocked with ReasonPurgeComplete.
func TestOrphanedLockRecoverer_UnlocksWhenWorkerNeverStarted(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-orphan-recoverer-never-started")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	jobID := uuid.New().String()

	sm := redissmq.NewStateManager()
	if _, err := sm.Lock(ctx, params, publicqueue.LockOwnerPurgeJob, jobID, nil); err != nil {
		t.Fatalf("lock: %v", err)
	}

	// Deliberately no JobWorker / WorkerHeartbeat seeding — the worker
	// "never started" from the recoverer's perspective.

	transition := waitForQueueState(t, params, publicqueue.StateActive, orphanedLockRecovererDeadline)

	if transition.Reason != publicqueue.TransitionReason(publicqueue.ReasonPurgeComplete) {
		t.Errorf("unlock reason = %q, want %q",
			transition.Reason, publicqueue.ReasonPurgeComplete)
	}
	if transition.LockID == nil || *transition.LockID != jobID {
		t.Errorf("unlock lockID = %v, want %q", transition.LockID, jobID)
	}
}

// TestOrphanedLockRecoverer_UnlocksWhenWorkerHeartbeatExpired covers the
// case where the purge worker started the job (JobWorker mapping exists)
// but its heartbeat expired — worker crashed mid-purge without cleanup.
// The recoverer sees JobWorker pointing at a dead worker and unlocks.
func TestOrphanedLockRecoverer_UnlocksWhenWorkerHeartbeatExpired(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-orphan-recoverer-heartbeat-expired")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	jobID := uuid.New().String()
	workerID := "dead-worker-" + uuid.New().String()

	sm := redissmq.NewStateManager()
	if _, err := sm.Lock(ctx, params, publicqueue.LockOwnerPurgeJob, jobID, nil); err != nil {
		t.Fatalf("lock: %v", err)
	}

	// JobWorker mapping present, heartbeat absent — equivalent to a
	// worker that started the job and crashed before (or during) its
	// first heartbeat refresh.
	c := internalredis.Client()
	if err := c.Set(ctx, keys.System{}.JobWorker(jobID), workerID, 0).Err(); err != nil {
		t.Fatalf("seed JobWorker: %v", err)
	}
	t.Cleanup(func() { c.Del(context.Background(), keys.System{}.JobWorker(jobID)) })

	waitForQueueState(t, params, publicqueue.StateActive, orphanedLockRecovererDeadline)
}

// TestOrphanedLockRecoverer_DoesNotUnlockLiveWorker verifies the
// recoverer's negative path: a healthy purge (live worker, active
// heartbeat) is not disturbed.
//
// Polls for the full deadline rather than a short check — the
// recoverer's first tick could land anywhere in [0, 60s], so a 5s
// observation would miss a bug that only fires on the recoverer's
// first tick. The whole window must be covered.
func TestOrphanedLockRecoverer_DoesNotUnlockLiveWorker(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-orphan-recoverer-live-worker")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	jobID := uuid.New().String()
	workerID := "live-worker-" + uuid.New().String()

	sm := redissmq.NewStateManager()
	if _, err := sm.Lock(ctx, params, publicqueue.LockOwnerPurgeJob, jobID, nil); err != nil {
		t.Fatalf("lock: %v", err)
	}

	seedLivePurgeWorker(t, jobID, workerID)

	end := time.Now().Add(orphanedLockRecovererDeadline)
	for time.Now().Before(end) {
		transition, err := sm.Current(ctx, params)
		if err != nil {
			t.Fatalf("current: %v", err)
		}
		if transition.To != publicqueue.StateLocked {
			t.Fatalf("healthy purge was unlocked: state = %v, reason = %v",
				transition.To, transition.Reason)
		}
		time.Sleep(time.Second)
	}
}

// TestOrphanedLockRecoverer_IsolatedAcrossQueues verifies per-queue
// isolation: a healthy locked queue and an orphaned locked queue are
// handled independently. Only the orphaned one unlocks.
func TestOrphanedLockRecoverer_IsolatedAcrossQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	healthy := publicqueue.MustQueueParams("test-orphan-recoverer-isolated-healthy")
	orphaned := publicqueue.MustQueueParams("test-orphan-recoverer-isolated-orphaned")
	testutil.CreateQueue(t, ctx, healthy, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, orphaned, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	healthyJobID := uuid.New().String()
	orphanedJobID := uuid.New().String()
	healthyWorkerID := "live-worker-" + uuid.New().String()

	sm := redissmq.NewStateManager()
	if _, err := sm.Lock(ctx, healthy, publicqueue.LockOwnerPurgeJob, healthyJobID, nil); err != nil {
		t.Fatalf("lock healthy: %v", err)
	}
	seedLivePurgeWorker(t, healthyJobID, healthyWorkerID)

	if _, err := sm.Lock(ctx, orphaned, publicqueue.LockOwnerPurgeJob, orphanedJobID, nil); err != nil {
		t.Fatalf("lock orphaned: %v", err)
	}
	// No seed for orphaned — its worker is dead.

	// Wait for orphaned to unlock. During this wait, healthy is
	// observed repeatedly by its own recoverer (both queues are in
	// the same coordinator's active set). If there were an isolation
	// bug, healthy would transition at the same tick as orphaned and
	// the check below would catch it.
	waitForQueueState(t, orphaned, publicqueue.StateActive, orphanedLockRecovererDeadline)

	current, err := sm.Current(ctx, healthy)
	if err != nil {
		t.Fatalf("current healthy: %v", err)
	}
	if current.To != publicqueue.StateLocked {
		t.Fatalf("healthy queue was unlocked: state = %v, reason = %v",
			current.To, current.Reason)
	}

	// Belt and braces: no Locked→Active transition anywhere in
	// healthy's history.
	history, err := sm.History(ctx, healthy)
	if err != nil {
		t.Fatalf("history healthy: %v", err)
	}
	for _, tr := range history {
		if tr.From != nil && *tr.From == publicqueue.StateLocked && tr.To == publicqueue.StateActive {
			t.Errorf("healthy queue has unexpected Locked→Active transition: reason=%q lockID=%v",
				tr.Reason, tr.LockID)
		}
	}
}

// --- helpers ---

// seedLivePurgeWorker makes the recoverer believe the given purge job
// is currently held by a live worker: JobWorker maps to the worker ID,
// and WorkerHeartbeat has a TTL long enough to outlast the test.
func seedLivePurgeWorker(t *testing.T, jobID, workerID string) {
	t.Helper()
	ctx := context.Background()
	c := internalredis.Client()

	if err := c.Set(ctx, keys.System{}.JobWorker(jobID), workerID, 0).Err(); err != nil {
		t.Fatalf("seed JobWorker(%s): %v", jobID, err)
	}
	if err := c.Set(ctx, keys.System{}.WorkerHeartbeat(workerID), "1", 10*time.Minute).Err(); err != nil {
		t.Fatalf("seed WorkerHeartbeat(%s): %v", workerID, err)
	}

	t.Cleanup(func() {
		c.Del(context.Background(),
			keys.System{}.JobWorker(jobID),
			keys.System{}.WorkerHeartbeat(workerID),
		)
	})
}

// waitForQueueState polls StateManager.Current until the queue reaches
// want or the deadline elapses. Returns the transition that reached
// want so callers can inspect Reason / LockID.
func waitForQueueState(
	t *testing.T,
	params *publicqueue.Params,
	want publicqueue.State,
	deadline time.Duration,
) *publicqueue.StateTransition {
	t.Helper()
	ctx := context.Background()
	sm := redissmq.NewStateManager()

	end := time.Now().Add(deadline)
	var last *publicqueue.StateTransition
	for time.Now().Before(end) {
		transition, err := sm.Current(ctx, params)
		if err != nil {
			t.Fatalf("current: %v", err)
		}
		last = transition
		if transition.To == want {
			return transition
		}
		time.Sleep(500 * time.Millisecond)
	}
	if last == nil {
		t.Fatalf("timeout waiting for state %v (never observed)", want)
	}
	t.Fatalf("timeout waiting for state %v, last seen %v", want, last.To)
	return nil
}

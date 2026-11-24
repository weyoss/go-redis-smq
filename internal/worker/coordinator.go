/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/weyoss/go-redis-smq/internal/eventbus"
	internalQueueEvents "github.com/weyoss/go-redis-smq/internal/queue/events"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/lock"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

const (
	// pendingQueuesCapacity bounds the queue.queueCreated event buffer.
	//
	// Sized for burst creation: 256 events at ~1ms per reconcile is
	// ~256ms of headroom. If the buffer fills (pathological burst
	// creation, or a stalled reconcile-queue worker), new events are
	// dropped and picked up by the next periodic sweep. Correctness
	// is preserved; only latency degrades.
	pendingQueuesCapacity = 256
)

// Coordinator manages per-queue worker leases and lifecycles at process
// scope. Spawned by redissmq.Init alongside PurgeWorker; stopped in
// redissmq.shutdownLocked.
//
// For each live queue, the coordinator attempts to acquire that queue's
// WorkersLock. Exactly one process in the cluster holds each lock at any
// time. The holder spawns the five background workers for that queue;
// non-holders do nothing.
//
// New queue discovery has two paths:
//
//   - Event-driven (primary): a queue.queueCreated subscription feeds
//     pendingQueues; a dedicated goroutine drains it and reconciles the
//     new queue immediately (sub-millisecond under normal load, plus one
//     SETNX round-trip). This preserves the pre-refactor responsiveness
//     for interactive use — create a queue, produce a message, expect
//     near-immediate worker processing.
//
//   - Periodic (fallback): every intervals.Reconcile, reconcileOnce scans
//     AllQueues(). Catches queues whose creation event was dropped
//     (buffer full, subscription failed at startup) and any queue that
//     survived a transient Redis outage during the event.
//
// Lock-loss detection: the per-queue lease is created with WithOnLost,
// which fires if the refresh loop observes ExtendLock returning 0
// (another process has taken the lock). onLost cancels the per-queue
// context, causing all five workers to exit within one refresh interval.
// The stale entry is detected on the next reconcile tick via
// queueWorkers.isStale() and torn down.
//
// Cadences are injected via Intervals (see DefaultIntervals /
// TestIntervals). The zero value is not usable; construct via
// NewCoordinator or NewCoordinatorWithIntervals.
type Coordinator struct {
	instanceID string
	intervals  Intervals
	log        *slog.Logger

	mu     sync.Mutex
	active map[string]*queueWorkers // queue.String() → live workers

	// createdSub is the queue.queueCreated subscription. Held so stop()
	// can Unsubscribe. nil if the subscription failed at startup (the
	// coordinator falls back to reconcile-only).
	createdSub *eventbus.Subscription

	// pendingQueues buffers queue.queueCreated events between the event
	// bus dispatch goroutine and the reconcile-queue worker. The handler
	// runs synchronously on the event bus goroutine and must not block
	// on Redis I/O — a slow SETNX during reconcileQueue would delay
	// every other system-bus event. The channel decouples them: the
	// handler does a non-blocking send, the worker does the SETNX.
	pendingQueues chan *queue.Params

	// teardownWg tracks async teardowns spawned by reconcileOnce.
	// The stop function waits on this after reconcileWg and createdWg,
	// so no teardown goroutine is still calling Redis when the stop
	// function returns.
	teardownWg sync.WaitGroup
}

// NewCoordinator creates a new coordinator with production cadences
// (DefaultIntervals). Equivalent to
// NewCoordinatorWithIntervals(DefaultIntervals()) — kept for callers
// that don't need to override anything.
func NewCoordinator() *Coordinator {
	return NewCoordinatorWithIntervals(DefaultIntervals())
}

// NewCoordinatorWithIntervals creates a coordinator with the given
// cadences. The instance ID (used as the lock owner for every per-queue
// lease this coordinator acquires) is always freshly generated — it is
// not configurable; a test that needs a stable ID should observe it via
// logging instead.
//
// Panics on invalid intervals. Misconfiguration of Intervals is a
// programmer error (production code passes DefaultIntervals; tests pass
// TestIntervals), so a panic is preferable to a subtle bug from, e.g.,
// LeaseRefresh >= LeaseTTL silently killing healthy leases.
func NewCoordinatorWithIntervals(intervals Intervals) *Coordinator {
	if err := intervals.Validate(); err != nil {
		panic(err)
	}
	return &Coordinator{
		instanceID:    uuid.New().String(),
		intervals:     intervals,
		active:        make(map[string]*queueWorkers),
		pendingQueues: make(chan *queue.Params, pendingQueuesCapacity),
		log:           logger.New("worker", "coordinator"),
	}
}

// Start spawns the coordinator and returns a stop function.
//
// Startup sequence:
//
//  1. Subscribe to queue.queueCreated before anything else, so a queue
//     created between subscription and the first reconcile pass is
//     caught by both paths (idempotent — trySpawnWorkers guards on the
//     lease).
//  2. Spawn the reconcile-queue worker goroutine that drains
//     pendingQueues. Event handler sends to the channel; worker calls
//     trySpawnWorkers.
//  3. Run one synchronous reconcile pass, handling every queue that
//     existed before Start was called.
//  4. Spawn the periodic reconcile loop.
//
// The returned stop function cancels the context, unsubscribes from
// queue.queueCreated, then waits on three goroutine groups in order:
// reconcile loop (no new teardowns after this), reconcile-queue worker
// (no new spawns after this), async teardowns. Then synchronously stops
// every remaining worker set. Same discipline as PurgeWorker.stop:
// unconditional waits, bounded by ctx cancellation rather than wall
// clock, so no coordinator-owned goroutine is calling Redis when
// redissmq.Shutdown proceeds to internalredis.Close.
func (c *Coordinator) Start(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)

	// Phase 1: event subscription. Non-fatal on failure — the periodic
	// sweep still handles new queues, just with latency.
	sub, err := internalQueueEvents.SubscribeCreated(func(p internalQueueEvents.CreatedPayload) {
		qp := p.Queue
		select {
		case c.pendingQueues <- &qp:
		default:
			c.log.Debug("reconcile queue buffer full, relying on periodic reconcile",
				"queue", qp.String(),
			)
		}
	})
	if err != nil {
		c.log.Error("subscribe to queue.created; falling back to reconcile-only",
			"error", err)
	} else {
		c.createdSub = sub
	}

	// Phase 2: reconcile-queue worker. Processes queue.queueCreated
	// events serially, off the event bus goroutine.
	var createdWg sync.WaitGroup
	createdWg.Add(1)
	go func() {
		defer createdWg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case qp := <-c.pendingQueues:
				c.reconcileQueue(ctx, qp)
			}
		}
	}()

	// Phase 3: synchronous first pass — existing queues handled before
	// Start returns. Deterministic for tests.
	c.reconcileOnce(ctx)

	// Phase 4: periodic reconcile loop.
	var reconcileWg sync.WaitGroup
	reconcileWg.Add(1)
	go func() {
		defer reconcileWg.Done()
		c.reconcileLoop(ctx)
	}()

	c.log.Info("coordinator started",
		"instanceID", c.instanceID,
		"reconcileInterval", c.intervals.Reconcile,
		"leaseTTL", c.intervals.LeaseTTL,
		"leaseRefresh", c.intervals.LeaseRefresh,
	)

	return func() {
		cancel()

		// Unsubscribe before waiting so no new events reach pendingQueues
		// after this point. An already-dispatched event might still send
		// to pendingQueues (the event bus doesn't wait for in-flight
		// handlers on Unsubscribe), but the reconcile-queue worker sees
		// the cancelled ctx and exits; the buffered message is harmlessly
		// discarded.
		if c.createdSub != nil {
			c.createdSub.Unsubscribe()
			c.createdSub = nil
		}

		reconcileWg.Wait()  // periodic loop exits; no new teardownWg.Add
		createdWg.Wait()    // reconcile-queue worker exits; no new spawns
		c.teardownWg.Wait() // in-flight teardowns from reconcileOnce finish

		c.mu.Lock()
		active := c.active
		c.active = nil
		c.mu.Unlock()

		// Stop any remaining worker sets. Some may be stale (lease lost,
		// reconcile hadn't yet torn them down); qw.stop() handles both
		// cases — cancel is a no-op if already cancelled, Release is a
		// no-op if already released.
		for key, qw := range active {
			if err := qw.stop(); err != nil {
				c.log.Error("stop queue workers on shutdown",
					"queue", key,
					"error", err,
				)
			}
		}

		c.log.Info("coordinator stopped", "instanceID", c.instanceID)
	}
}

func (c *Coordinator) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(c.intervals.Reconcile)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reconcileOnce(ctx)
		}
	}
}

// reconcileQueue spawns workers for a single newly-created queue. Called
// from the reconcile-queue worker goroutine in response to a
// queue.queueCreated event.
//
// Shares the "already active?" guard with reconcileOnce, but skips the
// AllQueues() scan. If reconcileOnce runs concurrently (e.g., the
// periodic tick fires while an event is being processed), both paths
// reach trySpawnWorkers — the lease is the serialization point (SETNX
// guarantees exactly one winner), so no additional locking is needed.
func (c *Coordinator) reconcileQueue(ctx context.Context, qp *queue.Params) {
	key := qp.String()

	c.mu.Lock()
	_, alreadyActive := c.active[key]
	c.mu.Unlock()
	if alreadyActive {
		return
	}

	c.trySpawnWorkers(ctx, qp)
}

// reconcileOnce scans AllQueues() and:
//   - Tears down workers for queues that no longer exist, and for queues
//     whose lease was lost (detected via queueWorkers.isStale()).
//   - Spawns workers for newly-discovered queues, or queues whose previous
//     worker set was torn down this tick.
//
// Existing healthy leases are untouched — worker crashes (via WithOnLost)
// make the entry stale, which the next reconcile cleans up.
func (c *Coordinator) reconcileOnce(ctx context.Context) {
	members, err := redisClient.Client().SMembers(ctx, keys.System{}.AllQueues()).Result()
	if err != nil {
		c.log.Error("enumerate queues", "error", err)
		return
	}

	seen := make(map[string]struct{}, len(members))
	for _, raw := range members {
		var qp queue.Params
		if err := json.Unmarshal([]byte(raw), &qp); err != nil {
			continue
		}
		seen[qp.String()] = struct{}{}
	}

	// Phase 1: collect teardown candidates — removed queues and stale
	// leases. Single c.mu acquisition. Record the keys so Phase 3 can
	// skip re-spawn for them (the async teardown hasn't finished yet;
	// re-spawning would race the release).
	tornDownKeys := make(map[string]struct{})
	c.mu.Lock()
	toTearDown := make([]*queueWorkers, 0)
	for key, qw := range c.active {
		_, queueExists := seen[key]
		stale := qw.isStale()

		if queueExists && !stale {
			continue
		}

		reason := "queue removed"
		if stale {
			reason = "lease lost"
		}
		c.log.Info("tearing down workers", "queue", key, "reason", reason)

		toTearDown = append(toTearDown, qw)
		tornDownKeys[key] = struct{}{}
		delete(c.active, key)
	}
	c.mu.Unlock()

	// Phase 2: async teardown, tracked by teardownWg so the stop function
	// can wait for in-flight teardowns. Each teardown runs outside c.mu
	// (qw.stop blocks on wg.Wait and lock.Release, neither of which
	// touches c.mu).
	for _, qw := range toTearDown {
		c.teardownWg.Add(1)
		go func(qw *queueWorkers) {
			defer c.teardownWg.Done()
			if err := qw.stop(); err != nil {
				c.log.Error("stop queue workers",
					"queue", qw.String(),
					"error", err,
				)
			}
		}(qw)
	}

	// Phase 3: spawn for newly-discovered queues (or queues whose previous
	// entry was already removed by a prior reconcile). Skip any queue torn
	// down this tick — its teardown is still in-flight; re-spawning now
	// would race the release and produce a spurious "already acquired"
	// error, or worse, a lease that's released after a new one is acquired.
	for _, raw := range members {
		var qp queue.Params
		if err := json.Unmarshal([]byte(raw), &qp); err != nil {
			continue
		}
		key := qp.String()

		if _, justTornDown := tornDownKeys[key]; justTornDown {
			continue
		}

		c.mu.Lock()
		_, alreadyActive := c.active[key]
		c.mu.Unlock()
		if alreadyActive {
			continue
		}

		c.trySpawnWorkers(ctx, &qp)
	}
}

// trySpawnWorkers attempts a non-blocking lease acquisition for the given
// queue and spawns the five workers on success. On failure (someone else
// holds the lock), returns silently — the next reconcile tick or event
// delivery retries.
//
// Note: if the queue is deleted between reconcileOnce's AllQueues() read
// and this Acquire(), the SETNX still succeeds (the lock key was DEL'd
// by delete-queue.lua). The five workers run against a missing queue for
// up to one reconcile interval; their scripts return QUEUE_NOT_FOUND or
// find nothing, and the next reconcile tears them down. Acceptable: the
// race window is ~1ms and the cost is five idle goroutines for at most
// one Reconcile interval.
//
// Ordering discipline:
//
//  1. Construct qw (no goroutines yet).
//  2. Wire lease with onLost → cancel (via closure).
//  3. Acquire lease. If it fails, cancel() and return — no entry created.
//  4. Insert into c.active BEFORE spawning. If onLost fires between
//     Acquire returning and the go statements below, cancel() has already
//     been called (workers exit immediately on their first ctx check),
//     and reconcile will detect the stale lease via isStale() and tear
//     down. Inserting before spawning keeps active as the single source
//     of truth — no window where a live lease has no active entry.
//  5. wg.Add(5), then spawn goroutines. Add is done before the go
//     statements per WaitGroup discipline; qw.wg.Wait() is safe to call
//     even if some goroutines haven't been scheduled yet.
func (c *Coordinator) trySpawnWorkers(ctx context.Context, qp *queue.Params) {
	key := qp.String()
	qKey := keys.Queue{Namespace: qp.NS(), Name: qp.Name()}

	workerCtx, cancel := context.WithCancel(ctx)

	qw := &queueWorkers{
		queue:  qp,
		cancel: cancel,
	}

	// Wire the lease's onLost callback to the per-queue cancel. The
	// callback fires from the lease's refresh loop, which runs inside the
	// lease; cancelling the per-queue context terminates all five workers.
	// Crucially, onLost does NOT touch c.active — deleting from active
	// here would race trySpawnWorkers inserting the entry (lease stolen
	// in the window between Acquire and active[key] = qw). Reconcile
	// detects stale entries via isStale() on its next tick.
	qw.lock = lock.New(
		qKey.WorkersLock(),
		c.instanceID,
		lock.WithTTL(c.intervals.LeaseTTL),
		lock.WithAutoRefresh(c.intervals.LeaseRefresh),
		lock.WithOnLost(func() {
			c.log.Warn("lease lost — cancelling workers",
				"queue", key,
				"instanceID", c.instanceID,
			)
			cancel()
		}),
	)

	if err := qw.lock.Acquire(ctx); err != nil {
		cancel()
		return
	}

	c.mu.Lock()
	c.active[key] = qw
	c.mu.Unlock()

	// Local worker instances — nothing outside this function needs them.
	scheduled := newScheduledPublisher(qp, c.intervals.ScheduledPublisher)
	delayed := newDelayedRequeuer(qp, c.intervals.DelayedRequeuer)
	immediate := newImmediateRequeuer(qp, c.intervals.ImmediateRequeuer)
	reaper := newReapConsumers(qp, c.intervals.Reaper)
	orphaned := newOrphanedLockRecoverer(qp, c.intervals.LockRecoverer)

	qw.wg.Add(5)
	go func() { defer qw.wg.Done(); scheduled.Run(workerCtx) }()
	go func() { defer qw.wg.Done(); delayed.Run(workerCtx) }()
	go func() { defer qw.wg.Done(); immediate.Run(workerCtx) }()
	go func() { defer qw.wg.Done(); reaper.Run(workerCtx) }()
	go func() { defer qw.wg.Done(); orphaned.Run(workerCtx) }()

	c.log.Info("lease acquired — workers started",
		"queue", key,
		"instanceID", c.instanceID,
	)
}

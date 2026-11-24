/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Package worker provides process-scoped background workers that perform
// periodic, cluster-coordinated maintenance for RedisSMQ queues.
//
// # Two Patterns Live in This Codebase
//
// RedisSMQ has exactly two kinds of background work. New work must be
// placed according to which of the two patterns it is — the choice of
// pattern (and therefore the choice of package) is not stylistic. Each
// pattern encodes a specific invariant that the other cannot enforce.
//
//	Pattern A — Leader election per queue (this package)
//	Pattern B — Competing consumers over a work queue (internal/queue/purge_worker.go)
//
// The rest of this doc explains the invariant each pattern enforces, the
// primitive that enforces it, and the decision criteria for placement.
//
// # Pattern A: Leader Election per Queue
//
// The Coordinator (this package) ensures that for each live queue, exactly
// one process across the cluster runs that queue's periodic workers. It is
// the right pattern when the work is:
//
//   - Unbounded — it runs forever, not to completion.
//   - Periodic — it fires on a tick, not in response to an event.
//   - Redundant-if-uncoordinated — running it on every process produces
//     duplicate work, corrupted state, or excessive Redis load.
//
// Primitive: per-queue SETNX on the WorkersLock key with a TTL, refreshed
// periodically. The holder spawns five workers; non-holders do nothing.
// Discovery is event-driven (queue.queueCreated) plus a periodic sweep of
// AllQueues to catch dropped events. Loss detection is the ExtendLock
// reply — a 0 means the lease was stolen, and the holder cancels its
// workers.
//
// Canonical examples (all in this package):
//
//   - scheduledPublisher — moves due scheduled messages (delays, CRON,
//     repeats) into pending. If two processes ran it concurrently,
//     repeating messages would be cloned twice and published twice.
//
//   - delayedRequeuer — moves due delayed messages (retry backoff) into
//     pending. If uncoordinated, the same message could be requeued
//     multiple times.
//
//   - immediateRequeuer — moves requeued messages into pending or
//     delayed. Same failure mode.
//
//   - reapConsumers — detects dead consumers (heartbeat expired) and
//     recovers their in-flight messages. Redundant execution wastes
//     Redis round-trips and duplicates event publishes.
//
//   - orphanedLockRecoverer — unlocks queues whose purge worker has
//     died. Idempotent by design, but coordinating it avoids the
//     N-way SCAN cost of every process doing it.
//
// All five workers are spawned for one queue, together, by
// Coordinator.trySpawnWorkers, and torn down together by queueWorkers.stop.
// The "five workers per queue" shape is intentional: it means the lease
// lifecycle is a single unit, and there is no way for one worker for a
// queue to be running without the others being running too.
//
// # Pattern B: Competing Consumers over a Work Queue
//
// internal/queue.PurgeWorker executes discrete, finite jobs enqueued in
// PendingPurgeJobs. It is the right pattern when the work is:
//
//   - Bounded — it has a definite start and end (a purge of N messages).
//   - Enqueued — a specific event triggers it, not a tick (a user calls
//     QueueManager.PurgeQueue).
//   - Distinguishable-per-execution — two "purge queue X" requests are
//     two distinct jobs, not two redundant executions of the same task.
//
// Primitive: BRPopLPush from PendingPurgeJobs to ActivePurgeJobs. Redis's
// atomic pop-and-move means each job goes to exactly one consumer. N
// processes → N concurrent purges. No election, no per-queue leases, no
// periodic ticks. Each process has exactly one PurgeWorker; idle cost is
// one blocked goroutine per process.
//
// Canonical example: internal/queue/purge_worker.go.
//
// # Why PurgeWorker Does Not Live Here
//
// It is tempting to "unify" the two patterns — move PurgeWorker into
// Coordinator and treat it as a sixth worker. This would be a regression.
// Concretely:
//
//  1. Work-distribution mismatch. PurgeWorker competes on a single global
//     queue (PendingPurgeJobs). If it were spawned per-queue inside
//     trySpawnWorkers, a process holding 100 leases would spawn 100
//     goroutines all blocked on the same BRPopLPush — and a purge job
//     enqueued for queue X would be picked by whichever of those 100
//     goroutines happened to win the race, not necessarily the goroutine
//     whose queue matches job.Payload.Queue. The current 1-per-process
//     model distributes jobs across processes; the merged model would
//     distribute them randomly across goroutines of whichever process
//     happens to have any leases.
//
//  2. Ownership mismatch. A purge job is enqueued by an arbitrary caller
//     (the user's process, potentially different from any process running
//     a coordinator) and locked on the queue it purges. Moving it under
//     the coordinator imposes a spurious constraint: only a process
//     holding queue X's lease may purge queue X. That process might not
//     exist, or might be on a different queue's lease. The two systems
//     are independent; merging them creates a dependency that neither
//     needs.
//
//  3. Recovery mismatch. PurgeWorker's recovery is job-level: on worker
//     crash, WorkerHeartbeat expires and recover-stuck-job.lua re-queues
//     the specific job with its Meta.Purged progress preserved. The
//     coordinator's recovery is lease-level: on lease loss, reconcile
//     tears down and re-spawns the five workers. Merging means forcing
//     the purge recovery machinery through the lease machinery (or
//     leaving both, which is worse — two recovery paths for the same
//     work).
//
//  4. Blocking mismatch. PurgeWorker blocks on BRPopLPush. The
//     coordinator's workers are non-blocking tickers. Sharing the
//     coordination model would either make the purge worker use a timer
//     (busy-poll, Redis load, per-process redundant polls) or make the
//     periodic workers use blocking pops (nonsensical — they have no
//     work to pop). Neither direction improves anything.
//
// The two patterns are not two variations of "background work." They are
// two different primitives answering two different questions:
//
//	Pattern A: "Which process is allowed to do X?"       → leader election
//	Pattern B: "Which process should do the next unit of Y?" → work queue
//
// A periodic task is X. A discrete job is Y. If a new task does not fall
// cleanly into one of these, the task is probably wrong, not the pattern.
//
// # Placement Decision Tree
//
// When adding new background work, answer these questions in order. The
// first one that resolves to "yes" determines the pattern.
//
//	Q1. Is the work triggered by a specific event (a user action, a
//	    one-off request, an external signal) rather than by a tick?
//	    → Pattern B. Enqueue it in a Redis list (or equivalent) and add
//	      a competing-consumer worker. Do NOT put it in Coordinator.
//
//	Q2. Does the work have a definite start and a definite end?
//	    → Pattern B. Same as Q1.
//
//	Q3. Does the work run forever, firing on a periodic tick?
//	    → Pattern A. Add it as a sixth worker in Coordinator's
//	      trySpawnWorkers, wired to a configurable interval in Intervals
//	      (default and test).
//
//	Q4. Does running the work on every process simultaneously cause
//	    duplicate work, corrupted state, or excessive Redis load?
//	    → Pattern A. The invariant "one process at a time per queue" is
//	      what Coordinator exists to enforce.
//
//	Q5. None of the above?
//	    → The task is probably a handler called from an existing flow,
//	      not a background worker. Background workers are for work that
//	      cannot be driven by the event that causes it (delays, retries,
//	      scheduled transitions, recovery scans). If the work CAN be
//	      driven by the triggering event, drive it from the triggering
//	      event. A worker that only runs when its trigger fires is not a
//	      worker — it is an asynchronous handler.
//
// # Invariants the Coordinator Relies On
//
// Three invariants hold for every worker in this package. New workers must
// uphold them or the coordinator's guarantees break:
//
//  1. Idempotent under split-brain. During the ~10s lease-loss window
//     (bounded by Intervals.LeaseRefresh), two processes may briefly run
//     the same worker for the same queue. Every worker must be safe under
//     this — every message transition must be gated by a Lua-level
//     check (LREM/ZREM/etc.) so only one executor actually transitions
//     it. This is what makes lease-TTL-based coordination safe despite
//     not being a true distributed lock.
//
//  2. Cancellable. Run(ctx) must return promptly on ctx.Done(). The
//     coordinator's teardown path waits on queueWorkers.wg, which only
//     returns when every worker's Run has returned. A worker that ignores
//     ctx cancellation will block every queue shutdown.
//
//  3. Bounded per-tick work. A single tick (one publishDue, one requeue,
//     one reap) must not process unbounded data. All current workers cap
//     their batch at 99 items or equivalent. This is what keeps shutdown
//     bounded: after ctx is cancelled, the worst-case time to finish the
//     current tick is one batch's worth of Redis operations, not the
//     entire category list.
//
// # Where New Code Goes
//
//   - A new periodic worker for every queue               → here, trySpawnWorkers
//   - A new periodic worker for a specific queue          → here, guard on queue name in the worker
//   - A new discrete job with enqueue + compete semantics → internal/queue, next to purge_worker.go
//   - An asynchronous action triggered by a specific event → neither; call it inline
//     (or via a short-lived goroutine) from the triggering site
//
// The cost of misplacing code is not a compile error — it is a subtle
// distributed-systems bug that surfaces only under scale. This doc exists
// so the placement is a conscious decision, not an accident of where the
// author happened to open a file.
package worker

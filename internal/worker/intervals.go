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
	"fmt"
	"time"
)

// Intervals configures every cadence the coordinator and its per-queue
// workers tick at. Production defaults match the pre-refactor per-consumer
// behaviour. Tests override these via redissmq.WithWorkerIntervals to keep
// the full suite under Go's 10-minute default binary timeout even under
// -race.
type Intervals struct {
	// Reconcile is how often the coordinator scans AllQueues() for new,
	// removed, or lease-stale queues.
	Reconcile time.Duration

	// ScheduledPublisher is how often the scheduled publisher moves due
	// scheduled messages (delays, CRON, repeats) into pending.
	ScheduledPublisher time.Duration

	// DelayedRequeuer is how often the delayed requeuer moves due delayed
	// messages (retry backoff) into pending.
	DelayedRequeuer time.Duration

	// ImmediateRequeuer is how often the immediate requeuer moves requeued
	// messages into pending or delayed.
	ImmediateRequeuer time.Duration

	// Reaper is how often the dead-consumer reaper scans for consumers
	// whose heartbeat has expired and recovers their in-flight messages.
	Reaper time.Duration

	// LockRecoverer is how often the orphaned-lock recoverer scans for
	// queues locked by a purge worker that has since died.
	LockRecoverer time.Duration

	// LeaseTTL is the WorkersLock key expiry.
	LeaseTTL time.Duration

	// LeaseRefresh is how often the coordinator extends its WorkersLock
	// lease. Must be strictly less than LeaseTTL — otherwise a live
	// worker's lease can expire between refreshes and be stolen.
	LeaseRefresh time.Duration
}

// DefaultIntervals is the production configuration. Matches the cadences
// the code used before intervals were made configurable.
func DefaultIntervals() Intervals {
	return Intervals{
		Reconcile:          30 * time.Second,
		ScheduledPublisher: 5 * time.Second,
		DelayedRequeuer:    5 * time.Second,
		ImmediateRequeuer:  5 * time.Second,
		Reaper:             30 * time.Second,
		LockRecoverer:      30 * time.Second,
		LeaseTTL:           30 * time.Second,
		LeaseRefresh:       10 * time.Second,
	}
}

// TestIntervals is a fast configuration for the test suite. Every cadence
// is 10-300x faster than production, so tests that used to wait 30-120s
// for a periodic tick now complete in 1-10s. Combined with the aggressive
// lease TTL, a Coordinator whose workers are left over from a previous
// test (after FlushAll) releases its lock within ~5s instead of 30s.
func TestIntervals() Intervals {
	return Intervals{
		Reconcile:          250 * time.Millisecond,
		ScheduledPublisher: 100 * time.Millisecond,
		DelayedRequeuer:    100 * time.Millisecond,
		ImmediateRequeuer:  100 * time.Millisecond,
		Reaper:             500 * time.Millisecond,
		LockRecoverer:      500 * time.Millisecond,
		LeaseTTL:           5 * time.Second,
		LeaseRefresh:       1 * time.Second,
	}
}

// Validate reports configuration errors that would cause subtle bugs:
// LeaseRefresh >= LeaseTTL causes a healthy worker's lease to expire
// between refreshes; zero intervals cause a panic in time.NewTicker.
func (i Intervals) Validate() error {
	if i.LeaseTTL <= 0 {
		return fmt.Errorf("worker: LeaseTTL must be > 0")
	}
	if i.LeaseRefresh <= 0 {
		return fmt.Errorf("worker: LeaseRefresh must be > 0")
	}
	if i.LeaseRefresh >= i.LeaseTTL {
		return fmt.Errorf(
			"worker: LeaseRefresh (%s) must be strictly less than LeaseTTL (%s)",
			i.LeaseRefresh, i.LeaseTTL,
		)
	}
	if i.Reconcile <= 0 {
		return fmt.Errorf("worker: Reconcile must be > 0")
	}
	if i.ScheduledPublisher <= 0 {
		return fmt.Errorf("worker: ScheduledPublisher must be > 0")
	}
	if i.DelayedRequeuer <= 0 {
		return fmt.Errorf("worker: DelayedRequeuer must be > 0")
	}
	if i.ImmediateRequeuer <= 0 {
		return fmt.Errorf("worker: ImmediateRequeuer must be > 0")
	}
	if i.Reaper <= 0 {
		return fmt.Errorf("worker: Reaper must be > 0")
	}
	if i.LockRecoverer <= 0 {
		return fmt.Errorf("worker: LockRecoverer must be > 0")
	}
	return nil
}

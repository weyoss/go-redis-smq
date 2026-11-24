/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package lock

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
)

// BackoffStrategy defines a retry backoff function.
// Return -1 to stop retrying, or a positive duration to wait before the next attempt.
type BackoffStrategy func(attempt int) time.Duration

// ExponentialBackoff returns a backoff strategy with jitter.
// Base is the initial delay. maxAttempts of 0 means unlimited retries.
func ExponentialBackoff(base time.Duration, maxAttempts int) BackoffStrategy {
	return func(attempt int) time.Duration {
		if maxAttempts > 0 && attempt >= maxAttempts {
			return -1
		}
		backoff := float64(base) * math.Pow(2, float64(attempt))
		jitter := rand.Float64() * float64(base)
		return time.Duration(backoff + jitter)
	}
}

// LinearBackoff returns a linear backoff strategy.
// maxAttempts of 0 means unlimited retries.
func LinearBackoff(base time.Duration, maxAttempts int) BackoffStrategy {
	return func(attempt int) time.Duration {
		if maxAttempts > 0 && attempt >= maxAttempts {
			return -1
		}
		return base * time.Duration(attempt+1)
	}
}

// FixedBackoff returns a fixed delay backoff strategy.
// maxAttempts of 0 means unlimited retries.
func FixedBackoff(delay time.Duration, maxAttempts int) BackoffStrategy {
	return func(attempt int) time.Duration {
		if maxAttempts > 0 && attempt >= maxAttempts {
			return -1
		}
		return delay
	}
}

// Lock represents a distributed lock using Redis Lua scripts.
//   - extend-lock.lua: PEXPIRE if owner matches
//   - release-lock.lua: DEL if owner matches
//
// # Loss detection
//
// When constructed with WithAutoRefresh (required for any use that expects
// the lock to outlive its TTL) AND WithOnLost, the refresh loop actively
// interprets the ExtendLock reply:
//
//   - 1 (owner still matches, PEXPIRE succeeded): lease is held; do nothing.
//   - 0 (owner mismatch, or key gone): lease was lost — another process
//     acquired the key after our TTL expired, or the key was DEL'd
//     externally. The refresh loop sets acquired = false (so IsAcquired()
//     and Release() observe the loss) and fires onLost exactly once.
//   - Eval error (Redis unreachable, timeout, wrong reply type): treated
//     as transient. The loop logs and retries on the next tick — a single
//     failed refresh is not proof of loss, and killing workers on every
//     blip would be worse than leaving them running for one more tick.
//
// Without WithOnLost, the refresh loop still detects loss (acquired = false)
// but does not fire any callback — the holder can poll IsAcquired() if it
// wants to react.
type Lock struct {
	client  *redis.Client
	key     string
	owner   string
	ttl     time.Duration
	refresh time.Duration

	retry   bool
	backoff BackoffStrategy

	mu       sync.Mutex
	acquired bool

	// stopCh and refreshDone are set together in Acquire (when auto-refresh
	// is enabled) and consumed together in Release. Both are guarded by mu.
	//
	// refreshDone is closed by refreshLoop on exit. Release waits on it
	// before doing its own Redis EVAL, guaranteeing the loop's in-flight
	// operations (which use the shared Redis client) have completed before
	// Release can race them, and before shutdown can nil the client.
	//
	// Using a per-Acquire channel pair, not a shared sync.WaitGroup, is
	// deliberate: a shared WaitGroup would let a concurrent Acquire's
	// Add(1) extend Release's Wait, causing Release to wait for a refresh
	// loop that hasn't even started yet (it will, once the SETNX after
	// Release's EVAL succeeds). Per-instance channels let Release wait
	// only on the loop it owns.
	stopCh      chan struct{}
	refreshDone chan struct{}

	// onLost, when non-nil, is invoked exactly once by refreshLoop if the
	// lease is observed as lost (ExtendLock returned 0). Captured under
	// mu and called outside — the callback must not re-enter the Lock.
	//
	// The callback runs from the refresh loop's goroutine. It should be
	// fast (typically: cancel a context and signal worker exit) and must
	// not block on anything that depends on the refresh loop exiting —
	// the loop's deferred close(refreshDone) fires only after onLost
	// returns.
	onLost func()

	log *slog.Logger
}

// Option configures a Lock.
type Option func(*Lock)

// WithTTL sets the lock's time-to-live.
func WithTTL(ttl time.Duration) Option {
	return func(l *Lock) {
		l.ttl = ttl
	}
}

// WithAutoRefresh enables periodic lock TTL refreshing.
// refresh is the interval between refreshes (should be < ttl).
func WithAutoRefresh(refresh time.Duration) Option {
	return func(l *Lock) {
		l.refresh = refresh
	}
}

// WithRetry enables automatic retry on acquisition failure.
func WithRetry(backoff BackoffStrategy) Option {
	return func(l *Lock) {
		l.retry = true
		l.backoff = backoff
	}
}

// WithOnLost registers a callback that fires exactly once, from the
// refresh loop, if the lock is observed as lost — ExtendLock returns 0
// (owner mismatch, or key gone).
//
// Only fires when WithAutoRefresh is also set: without a refresh loop,
// there is no ExtendLock call and no loss-detection opportunity.
//
// The callback is invoked without holding the Lock's mutex. It should
// not call back into the Lock (Acquire/Release/IsAcquired are safe but
// pointless — acquired is already false; Release will short-circuit).
func WithOnLost(fn func()) Option {
	return func(l *Lock) {
		l.onLost = fn
	}
}

// New creates a new distributed lock.
// Uses the singleton Redis client for all operations.
func New(key, owner string, opts ...Option) *Lock {
	l := &Lock{
		client: redisClient.Client(),
		key:    key,
		owner:  owner,
		ttl:    30 * time.Second,
		log:    logger.New("lock"),
	}
	for _, o := range opts {
		o(l)
	}
	return l
}

// Acquire attempts to acquire the lock via SET NX.
// If WithRetry is configured, it will retry on failure with backoff.
func (l *Lock) Acquire(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.acquired {
		return fmt.Errorf("lock: already acquired")
	}

	attempt := 0
	for {
		ok, err := l.tryAcquire(ctx)
		if err != nil {
			return fmt.Errorf("lock: acquire: %w", err)
		}
		if ok {
			l.acquired = true

			// Start auto-refresh if configured. Each Acquire creates a
			// fresh stopCh/refreshDone pair; a subsequent Acquire after a
			// Release creates different channels. Release captures both
			// under l.mu and waits only on the instance it owns.
			if l.refresh > 0 {
				stopCh := make(chan struct{})
				done := make(chan struct{})
				l.stopCh = stopCh
				l.refreshDone = done
				go l.refreshLoop(stopCh, done)
			}

			return nil
		}

		if !l.retry || l.backoff == nil {
			return fmt.Errorf("lock: failed to acquire %s", l.key)
		}

		delay := l.backoff(attempt)
		if delay < 0 {
			return fmt.Errorf("lock: failed to acquire %s after %d attempts", l.key, attempt)
		}

		attempt++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// Release releases the lock using the release-lock.lua script.
// Only deletes the key if we own it.
//
// Order matters. The sequence is:
//
//  1. Capture stopCh/refreshDone under l.mu, nil out the fields, set
//     acquired = false. Release l.mu.
//  2. Close stopCh — signals the refresh loop to exit.
//  3. Wait on <-refreshDone — guarantees the loop has finished, including
//     any in-flight extend EVAL currently touching Redis.
//  4. Do our own release EVAL.
//
// Skipping step 3 would race the release EVAL with the refresh loop's
// extend EVAL — and, at shutdown, would leave the refresh loop calling
// Redis after internalredis.Close() nils the client (panic in an
// unrecovered goroutine — process crash).
//
// Holding l.mu across step 3 would deadlock: the refresh loop acquires
// l.mu inside its tick handler, so holding it while waiting would prevent
// the loop from ever completing. That's why the field capture (step 1)
// and the wait (step 3) are in separate lock acquisitions, with the lock
// released in between.
//
// The wait is unbounded, matching the C7 discipline (see
// MessageHandler.Shutdown's doc). It is bounded in practice by the Redis
// client's own dial/read/write timeouts — an extend EVAL that hangs on an
// unreachable Redis will error at the transport layer and the loop will
// exit shortly after. A timeout on this wait would reintroduce the race
// it's here to prevent.
//
// Short-circuit: if acquired is already false (loss detected by
// refreshLoop, or a prior Release), returns nil immediately without
// touching Redis. This is the correct behavior after lease loss — the
// winner owns the key now; a release EVAL would either be a no-op (owner
// mismatch — release-lock.lua's guard) or, in the same-owner case, would
// wrongly DEL the winner's key. Short-circuiting avoids the round-trip
// and the risk.
func (l *Lock) Release(ctx context.Context) error {
	l.mu.Lock()
	if !l.acquired {
		l.mu.Unlock()
		return nil
	}

	l.acquired = false
	stopCh := l.stopCh
	done := l.refreshDone
	l.stopCh = nil
	l.refreshDone = nil
	l.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	if done != nil {
		<-done
	}

	_, err := redisClient.Eval(ctx, scripts.ReleaseLock, []string{l.key}, l.owner)
	if err != nil {
		return fmt.Errorf("lock: release: %w", err)
	}

	return nil
}

// IsAcquired returns whether the lock is currently held.
//
// Returns false after a loss is detected by the refresh loop — even if
// Release has not been called. Callers using the lock as a liveness
// signal (e.g. queueWorkers.isStale) rely on this.
func (l *Lock) IsAcquired() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acquired
}

// tryAcquire attempts a single SET NX acquisition.
func (l *Lock) tryAcquire(ctx context.Context) (bool, error) {
	ok, err := l.client.SetNX(ctx, l.key, l.owner, l.ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// refreshLoop periodically extends the lock TTL using the
// extend-lock.lua script: PEXPIRE if owner matches.
//
// stopCh and done are per-instance channels created by the Acquire call
// that spawned this goroutine. Passing them as parameters (instead of
// reading l.stopCh/l.refreshDone) means the goroutine never touches a
// field that Release writes without holding the mutex, and never observes
// channels belonging to a different Acquire.
//
// done is closed on exit; Release waits on it to guarantee no in-flight
// Redis EVAL survives past Release's own Redis operations.
//
// Loss detection: the ExtendLock reply is interpreted, not discarded.
// A reply of 0 means the key is no longer ours (owner mismatch, or key
// gone — either way, loss). The loop sets acquired = false, captures the
// onLost callback under the mutex, releases the mutex, fires the callback,
// and returns. Release and IsAcquired will observe the loss.
//
// Transient Eval errors are NOT treated as loss: a single failed refresh
// is not proof the key was taken; the loop logs and retries on the next
// tick. Only an actual 0 reply triggers the loss path.
func (l *Lock) refreshLoop(stopCh <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(l.refresh)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			// Hold l.mu across the Eval so Release cannot proceed between
			// the acquired check and the ExtendLock call — that window
			// would let Release delete the key, causing ExtendLock to
			// return 0 and falsely report loss on a clean shutdown.
			l.mu.Lock()
			if !l.acquired {
				l.mu.Unlock()
				return
			}
			reply, err := redisClient.Eval(
				context.Background(),
				scripts.ExtendLock,
				[]string{l.key},
				l.owner,
				l.ttl.Milliseconds(),
			)
			if err != nil {
				l.mu.Unlock()
				l.log.Error("failed to extend lock",
					"key", l.key,
					"owner", l.owner,
					"error", err,
				)
				continue
			}

			n, intErr := redisClient.Int64(reply)
			if intErr != nil {
				l.mu.Unlock()
				l.log.Error("extend lock: unexpected reply",
					"key", l.key,
					"owner", l.owner,
					"reply", reply,
					"error", intErr,
				)
				continue
			}

			if n == 1 {
				l.mu.Unlock()
				continue
			}

			// ExtendLock returned 0 — the key is no longer ours.
			// Set acquired = false so IsAcquired() and Release() observe
			// the loss. Capture onLost under the mutex, fire it outside.
			l.acquired = false
			cb := l.onLost
			l.mu.Unlock()

			l.log.Warn("lock lost — another process acquired it",
				"key", l.key,
				"owner", l.owner,
			)

			if cb != nil {
				cb()
			}
			return
		}
	}
}

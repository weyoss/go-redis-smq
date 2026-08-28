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
type Lock struct {
	client  *redis.Client // singleton Redis client
	key     string
	owner   string
	ttl     time.Duration
	refresh time.Duration

	// Retry configuration
	retry   bool
	backoff BackoffStrategy

	mu       sync.Mutex
	acquired bool
	stopCh   chan struct{}

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

// WithAutoRefresh enables periodic lock TTL refreshing using the
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

			// Start auto-refresh if configured
			if l.refresh > 0 {
				l.stopCh = make(chan struct{})
				go l.refreshLoop()
			}

			return nil
		}

		// Lock not acquired — retry if configured
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
func (l *Lock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.acquired {
		return nil
	}

	// Stop auto-refresh
	if l.stopCh != nil {
		close(l.stopCh)
		l.stopCh = nil
	}

	// Use release-lock.lua: DEL if owner matches
	_, err := redisClient.Eval(ctx, scripts.ReleaseLock, []string{l.key}, l.owner)
	if err != nil {
		return fmt.Errorf("lock: release: %w", err)
	}

	l.acquired = false
	return nil
}

// IsAcquired returns whether the lock is currently held.
func (l *Lock) IsAcquired() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acquired
}

// tryAcquire attempts a single SET NX acquisition.
func (l *Lock) tryAcquire(ctx context.Context) (bool, error) {
	// SET key owner NX PX ttl
	ok, err := l.client.SetNX(ctx, l.key, l.owner, l.ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// refreshLoop periodically extends the lock TTL using the
// extend-lock.lua script: PEXPIRE if owner matches.
func (l *Lock) refreshLoop() {
	ticker := time.NewTicker(l.refresh)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			l.mu.Lock()
			if l.acquired {
				if _, err := redisClient.Eval(
					context.Background(),
					scripts.ExtendLock,
					[]string{l.key},
					l.owner,
					l.ttl.Milliseconds(),
				); err != nil {
					l.log.Error("failed to extend lock",
						"key", l.key,
						"owner", l.owner,
						"error", err,
					)
				}
			}
			l.mu.Unlock()
		}
	}
}

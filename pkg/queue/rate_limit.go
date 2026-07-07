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

	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// SetRateLimit sets a rate limit on a queue.
func (m *Manager) SetRateLimit(ctx context.Context, params *q.QueueParams, rl *q.RateLimitParams) error {
	return m.store.SetRateLimit(ctx, params, rl)
}

// ClearRateLimit removes the rate limit from a queue.
func (m *Manager) ClearRateLimit(ctx context.Context, params *q.QueueParams) error {
	return m.store.ClearRateLimit(ctx, params)
}

// RateLimit returns the current rate limit for a queue, or nil if none is set.
func (m *Manager) RateLimit(ctx context.Context, params *q.QueueParams) (*q.RateLimitParams, error) {
	return m.store.GetRateLimit(ctx, params)
}

// Package-level convenience functions using the default manager.

// SetRateLimit sets a rate limit using the default manager.
func SetRateLimit(ctx context.Context, params *q.QueueParams, rl *q.RateLimitParams) error {
	return defaultManager.SetRateLimit(ctx, params, rl)
}

// ClearRateLimit clears the rate limit using the default manager.
func ClearRateLimit(ctx context.Context, params *q.QueueParams) error {
	return defaultManager.ClearRateLimit(ctx, params)
}

// RateLimit returns the rate limit using the default manager.
func RateLimit(ctx context.Context, params *q.QueueParams) (*q.RateLimitParams, error) {
	return defaultManager.RateLimit(ctx, params)
}

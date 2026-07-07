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

// MustExist validates that a queue exists.
func (m *Manager) MustExist(ctx context.Context, params *q.QueueParams) error {
	return m.validator.Exists(ctx, params)
}

// MustBeOperational validates that a queue exists and is in an operational state.
func (m *Manager) MustBeOperational(ctx context.Context, params *q.QueueParams) error {
	return m.validator.IsOperational(ctx, params)
}

// CanEnqueue validates that a queue can accept new messages.
func (m *Manager) CanEnqueue(ctx context.Context, params *q.QueueParams) error {
	return m.validator.CanEnqueue(ctx, params)
}

// CanDequeue validates that a queue can deliver messages.
func (m *Manager) CanDequeue(ctx context.Context, params *q.QueueParams) error {
	return m.validator.CanDequeue(ctx, params)
}

// Package-level convenience functions.

func MustExist(ctx context.Context, params *q.QueueParams) error {
	return defaultManager.MustExist(ctx, params)
}

func MustBeOperational(ctx context.Context, params *q.QueueParams) error {
	return defaultManager.MustBeOperational(ctx, params)
}

func CanEnqueue(ctx context.Context, params *q.QueueParams) error {
	return defaultManager.CanEnqueue(ctx, params)
}

func CanDequeue(ctx context.Context, params *q.QueueParams) error {
	return defaultManager.CanDequeue(ctx, params)
}

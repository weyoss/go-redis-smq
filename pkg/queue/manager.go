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
)

// Manager is the public interface for queue-related operations.
//
// It provides methods for creating, inspecting, deleting, browsing,
// purging, and managing the state and rate limits of queues. The
// concrete implementation is provided by the root redissmq package.
type Manager interface {
	// Create creates a new queue with the specified type and delivery model.
	Create(ctx context.Context, params *Params, queueType Type, deliveryModel DeliveryModel) error

	// CreateWithRateLimit creates a new queue with rate limiting enabled.
	CreateWithRateLimit(ctx context.Context, params *Params, queueType Type, deliveryModel DeliveryModel, rl *RateLimitParams) error

	// Properties retrieves the stored configuration of a queue.
	Properties(ctx context.Context, params *Params) (*Props, error)

	// Exists checks whether a queue exists.
	Exists(ctx context.Context, params *Params) (bool, error)

	// Delete removes a queue and all associated data.
	Delete(ctx context.Context, params *Params) error

	// ListAll returns all queues across all namespaces.
	ListAll(ctx context.Context) ([]Params, error)

	// ListByNamespace returns all queues within a namespace.
	ListByNamespace(ctx context.Context, namespace string) ([]Params, error)

	// BrowseMessages returns a page of message IDs from a queue.
	BrowseMessages(ctx context.Context, queueParams *Params, params *BrowseParams) (*BrowseResult, error)

	// PurgeQueue enqueues a background purge job for a queue.
	PurgeQueue(ctx context.Context, queueParams *Params, filter BrowseFilter) (string, error)

	// GetPurgeJob returns a purge job by its ID.
	GetPurgeJob(ctx context.Context, jobID string) (*PurgeJob, error)

	// CancelPurgeJob cancels a pending or running purge job.
	CancelPurgeJob(ctx context.Context, queueParams *Params, jobID string) error

	// SetRateLimit sets a rate limit on a queue.
	SetRateLimit(ctx context.Context, params *Params, rl *RateLimitParams) error

	// ClearRateLimit removes the rate limit from a queue.
	ClearRateLimit(ctx context.Context, params *Params) error

	// RateLimit returns the current rate limit for a queue, or nil if none is set.
	RateLimit(ctx context.Context, params *Params) (*RateLimitParams, error)

	// MustExist validates that a queue exists.
	MustExist(ctx context.Context, params *Params) error

	// MustBeOperational validates that a queue exists and is operational.
	MustBeOperational(ctx context.Context, params *Params) error

	// CanEnqueue validates that a queue can accept new messages.
	CanEnqueue(ctx context.Context, params *Params) error

	// CanDequeue validates that a queue can deliver messages.
	CanDequeue(ctx context.Context, params *Params) error
}

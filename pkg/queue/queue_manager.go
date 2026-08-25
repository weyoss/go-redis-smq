/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package queue provides the public API for managing RedisSMQ queues.
package queue

import (
	"context"
)

// QueueManager is the public interface for queue-related operations.
//
// It provides methods for creating, inspecting, deleting, browsing,
// purging, and managing the state and rate limits of queues. The
// concrete implementation is provided by the root redissmq package.
type QueueManager interface {
	// Create creates a new queue with the specified type and delivery model.
	Create(ctx context.Context, params *QueueParams, queueType QueueType, deliveryModel DeliveryModel) error

	// CreateWithRateLimit creates a new queue with rate limiting enabled.
	CreateWithRateLimit(ctx context.Context, params *QueueParams, queueType QueueType, deliveryModel DeliveryModel, rl *RateLimitParams) error

	// Properties retrieves the stored configuration of a queue.
	Properties(ctx context.Context, params *QueueParams) (*QueueProps, error)

	// Exists checks whether a queue exists.
	Exists(ctx context.Context, params *QueueParams) (bool, error)

	// Delete removes a queue and all associated data.
	Delete(ctx context.Context, params *QueueParams) error

	// ListAll returns all queues across all namespaces.
	ListAll(ctx context.Context) ([]QueueParams, error)

	// ListByNamespace returns all queues within a namespace.
	ListByNamespace(ctx context.Context, namespace string) ([]QueueParams, error)

	// BrowseMessages returns a page of message IDs from a queue.
	BrowseMessages(ctx context.Context, queueParams *QueueParams, params *BrowseParams) (*BrowseResult, error)

	// PurgeQueue enqueues a background purge job for a queue.
	PurgeQueue(ctx context.Context, queueParams *QueueParams, filter BrowseFilter) (string, error)

	// GetPurgeJob returns a purge job by its ID.
	GetPurgeJob(ctx context.Context, jobID string) (*PurgeJob, error)

	// CancelPurgeJob cancels a pending or running purge job.
	CancelPurgeJob(ctx context.Context, queueParams *QueueParams, jobID string) error

	// SetRateLimit sets a rate limit on a queue.
	SetRateLimit(ctx context.Context, params *QueueParams, rl *RateLimitParams) error

	// ClearRateLimit removes the rate limit from a queue.
	ClearRateLimit(ctx context.Context, params *QueueParams) error

	// RateLimit returns the current rate limit for a queue, or nil if none is set.
	RateLimit(ctx context.Context, params *QueueParams) (*RateLimitParams, error)

	// MustExist validates that a queue exists.
	MustExist(ctx context.Context, params *QueueParams) error

	// MustBeOperational validates that a queue exists and is operational.
	MustBeOperational(ctx context.Context, params *QueueParams) error

	// CanEnqueue validates that a queue can accept new messages.
	CanEnqueue(ctx context.Context, params *QueueParams) error

	// CanDequeue validates that a queue can deliver messages.
	CanDequeue(ctx context.Context, params *QueueParams) error
}

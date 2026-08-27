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
//
// It defines the data types, interfaces, and sentinel errors needed to
// create, inspect, delete, pause, resume, and manage queues. The package
// itself does not implement queue storage or state management; those are
// provided by the root redissmq package through factory functions.
//
// # Concrete Implementations
//
// Use the following factory functions to obtain concrete managers:
//
//   - redissmq.NewQueueManager() returns a queue.Manager
//   - redissmq.NewStateManager() returns a queue.StateManager
//   - redissmq.NewConsumerGroupManager() returns a queue.ConsumerGroupManager
//
// # Queue Types and Delivery Models
//
// The package defines Type (FIFO, LIFO, Priority) and DeliveryModel
// (Point-to-Point, Pub/Sub) to control how messages are ordered and
// delivered.
//
// # Queue Parameters
//
// A Params identifies a queue by its name and optional namespace. Create
// one with NewQueueParams (uses the default namespace from configuration) or
// NewQueueParamsWithNS (explicit namespace). The Must* variants panic on
// error and are intended for testing or when parameters are known to be
// valid.
//
// # State Management
//
// The StateManager interface allows you to transition queues between
// Active, Paused, Stopped, and Locked states. It also provides access to
// state history and current state information.
//
// # Rate Limiting
//
// Rate limits can be applied to queues via Manager.SetRateLimit,
// ClearRateLimit, and RateLimit methods. RateLimitParams are used to define
// the limit and interval.
//
// # Browsing and Purging
//
// Manager.BrowseMessages allows paginated inspection of published,
// pending, scheduled, acknowledged, and dead-lettered messages (where audit
// is enabled). PurgeQueue enqueues a background job to delete messages of a
// given category.
//
// # Example
//
//	qm := redissmq.NewQueueManager()
//	params := queue.MustQueueParams("orders")
//	if err := qm.Create(ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
//	    log.Fatal(err)
//	}
package queue

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package consumer provides the public API for creating and managing
// RedisSMQ consumers.
//
// A consumer subscribes to one or more queues and processes messages using
// user-defined handlers. It manages heartbeats, background workers, batch
// acknowledgments/unacknowledgments, and graceful shutdown automatically.
//
// The concrete implementation is provided by the root redissmq package and is
// created using redissmq.NewConsumer(). The consumer returned by that factory
// implements the Consumer interface defined here.
//
// # Handlers
//
// A handler is a function with the signature:
//
//	func(ctx context.Context, m *message.Transferable) error
//
// Returning nil acknowledges the message. Returning an error triggers the
// retry mechanism (if configured) or ultimately moves the message to the
// dead-letter queue.
//
// # Lifecycle
//
// A consumer must be started with Run(ctx) after at least one handler has
// been registered via Consume or ConsumeWithGroup. The provided context is
// used to control the consumer's lifetime; cancelling the context triggers a
// graceful shutdown.
//
// Shutdown can also be called explicitly. It is safe to call multiple times.
//
// # Options
//
// The consumer can be configured using functional options:
//
//	consumer.WithHeartbeatTTL(30 * time.Second)
//	consumer.WithBatchAcks(consumer.BatchConfig{...})
//	consumer.WithBatchUnacks(consumer.BatchConfig{...})
//
// # Handler Management
//
// Handlers can be added or removed dynamically even while the consumer is
// running:
//
//	cons.Consume(queue, handler)          // add or replace a handler
//	cons.Cancel(queue)                    // remove a handler
//	cons.ConsumeWithGroup(queue, "grp", h) // add a handler for a consumer group
//	cons.CancelWithGroup(queue, "grp")    // remove a group handler
//
// # Consumer Groups
//
// For Pub/Sub queues, use ConsumeWithGroup to receive messages as part of a
// named consumer group. If no group ID is provided, an ephemeral group is
// created automatically and deleted on shutdown.
//
// # Example
//
//	cons := redissmq.NewConsumer()
//	cons.Consume(ordersQueue, func(ctx context.Context, m *msg.Transferable) error {
//	    log.Printf("Received: %v", m.Body)
//	    return nil
//	})
//	if err := cons.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer cons.Shutdown()
package consumer

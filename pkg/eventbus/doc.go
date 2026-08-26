/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package eventbus defines the public interfaces and subscription types used
// to observe RedisSMQ system events.
//
// Events are delivered over Redis Pub/Sub and are intended for monitoring,
// alerting, and integration. The package itself contains only interfaces and
// a lightweight global registry; the concrete event bus implementation is
// provided internally and wired by the root redissmq package.
//
// # Public vs System Bus
//
// RedisSMQ maintains two separate event buses:
//
//   - System bus – used internally for cross‑instance synchronisation and
//     component communication. It is not exposed to external callers.
//   - User bus – the public bus that delivers events to external subscribers.
//     It is started by calling redissmq.InitUserEventBus(ctx) and is made
//     available through this package.
//
// # Subscribing
//
// Domain packages (queue, producer, consumer) provide typed subscription
// functions that use the public user bus. Those functions return a
// Subscription, which should be unsubscribed when no longer needed:
//
//	sub, err := queueEvents.SubscribeCreated(handler)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer sub.Unsubscribe()
//
// # Event Bus Interface
//
// The EventBus interface defines the minimal contract required to subscribe
// to events. It is satisfied by the internal bus adapter provided by the
// root redissmq package. You should not need to implement this interface
// yourself.
//
// # Thread Safety
//
// The global user bus registry is safe for concurrent use. However,
// subscription handlers are invoked synchronously and should be kept fast
// and non‑blocking.
package eventbus

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package eventbus

import "sync"

// Subscription represents a subscription to an event.
// It allows the caller to stop receiving events.
type Subscription interface {
	Unsubscribe()
}

// EventBus is the minimal interface required for subscribing to RedisSMQ
// events. The concrete implementation is provided by the root redissmq
// package.
type EventBus interface {
	Subscribe(handler func(eventName string, args []interface{}), eventName string) (Subscription, error)
}

// userBusMu guards userBus. SetUserBus is called rarely — once from
// redissmq.InitUserEventBus, once from redissmq.ShutdownUserEventBus. UserBus
// is called from every SubscribeXxx in the domain packages. An uncontended
// RLock is a few nanoseconds, dwarfed by the Redis round-trip that follows in
// every real call site.
var (
	userBusMu sync.RWMutex
	userBus   EventBus
)

// SetUserBus sets the public user event bus used by public subscription
// functions. It is intended to be called by the root redissmq package during
// initialization. Passing nil clears the bus.
//
// Safe for concurrent use with UserBus and with itself.
func SetUserBus(bus EventBus) {
	userBusMu.Lock()
	defer userBusMu.Unlock()
	userBus = bus
}

// UserBus returns the current public user event bus, or nil if not
// configured.
//
// Safe for concurrent use with SetUserBus and with itself.
func UserBus() EventBus {
	userBusMu.RLock()
	defer userBusMu.RUnlock()
	return userBus
}

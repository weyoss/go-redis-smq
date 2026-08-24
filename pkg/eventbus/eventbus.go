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

// userBus holds the currently configured public user event bus.
var userBus EventBus

// SetUserBus sets the public user event bus used by public subscription
// functions. It is intended to be called by the root redissmq package during
// initialization.
func SetUserBus(bus EventBus) {
	userBus = bus
}

// UserBus returns the current public user event bus, or nil if not
// configured.
func UserBus() EventBus {
	return userBus
}

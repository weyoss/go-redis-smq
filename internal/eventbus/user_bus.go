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

import (
	"context"
	"sync"
)

var (
	userBusMu       sync.Mutex
	userBusInstance *EventBus
)

// InitUser creates and starts the public/user event bus.
//
// Unlike the system bus, the user bus is not started automatically during
// RedisSMQ bootstrap. Applications that need to expose events to external
// subscribers must call InitUser explicitly.
//
// If the user bus was previously shut down, calling InitUser again will
// create and start a fresh instance.
func InitUser(ctx context.Context) *EventBus {
	userBusMu.Lock()
	defer userBusMu.Unlock()

	if userBusInstance == nil {
		userBusInstance = NewEventBus("user")
		userBusInstance.Start(ctx)
	}
	return userBusInstance
}

// User returns the public/user event bus singleton.
//
// It returns nil if the user bus has not been initialised.
func User() *EventBus {
	userBusMu.Lock()
	defer userBusMu.Unlock()
	return userBusInstance
}

// ShutdownUser shuts down the user bus and clears the singleton.
//
// After calling ShutdownUser, the application may call InitUser again to
// create a new user bus instance.
func ShutdownUser() {
	userBusMu.Lock()
	defer userBusMu.Unlock()

	if userBusInstance != nil {
		userBusInstance.Shutdown()
		userBusInstance = nil
	}
}

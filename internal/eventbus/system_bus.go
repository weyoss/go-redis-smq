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
	systemBusMu       sync.Mutex
	systemBusInstance *EventBus
)

// InitSystem creates and starts the internal/system event bus.
//
// This bus is used for cross-instance synchronisation and internal
// component communication. It must be called during RedisSMQ bootstrap.
//
// It is safe to call InitSystem multiple times. If the system bus was
// previously shut down, InitSystem will create and start a new instance.
func InitSystem(ctx context.Context) *EventBus {
	systemBusMu.Lock()
	defer systemBusMu.Unlock()

	if systemBusInstance == nil {
		systemBusInstance = NewEventBus("system")
		systemBusInstance.Start(ctx)
	}
	return systemBusInstance
}

// System returns the internal/system event bus singleton.
//
// It panics if InitSystem has not been called or if the system bus has been
// shut down.
func System() *EventBus {
	systemBusMu.Lock()
	defer systemBusMu.Unlock()

	if systemBusInstance == nil {
		panic("eventbus: system bus not initialized — call InitSystem first")
	}
	return systemBusInstance
}

// ShutdownSystem shuts down the system bus and clears the singleton.
//
// After calling ShutdownSystem, the application may call InitSystem again
// to create a new system bus instance.
func ShutdownSystem() {
	systemBusMu.Lock()
	defer systemBusMu.Unlock()

	if systemBusInstance != nil {
		systemBusInstance.Shutdown()
		systemBusInstance = nil
	}
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package eventmultiplexer

import (
	"context"

	"github.com/weyoss/go-redis-smq/internal/eventbus"
)

// Target defines the destination bus for an event.
type Target int

const (
	// TargetSystem routes the event only to the internal system bus.
	TargetSystem Target = iota

	// TargetUser routes the event only to the public user bus.
	TargetUser

	// TargetBoth routes the event to both the system and user buses.
	TargetBoth
)

// Publish routes an event to the system bus, user bus, or both,
// based on the routing policy and whether the user bus is running.
//
// Internal synchronisation events are always sent to the system bus.
// Public monitoring events are sent to the user bus only when it has
// been initialised via eventbus.InitUser.
//
// The arguments must match the TypeScript event handler signature and are
// serialised as a JSON array on the wire.
func Publish(ctx context.Context, eventName string, args ...interface{}) error {
	target, ok := routingPolicies[eventName]
	if !ok {
		target = TargetUser
	}

	switch target {
	case TargetSystem:
		return eventbus.System().Publish(ctx, eventName, args...)
	case TargetUser:
		if userBus := eventbus.User(); userBus != nil && userBus.IsRunning() {
			return userBus.Publish(ctx, eventName, args...)
		}
		return nil
	case TargetBoth:
		if err := eventbus.System().Publish(ctx, eventName, args...); err != nil {
			return err
		}
		if userBus := eventbus.User(); userBus != nil && userBus.IsRunning() {
			return userBus.Publish(ctx, eventName, args...)
		}
		return nil
	}
	return nil
}

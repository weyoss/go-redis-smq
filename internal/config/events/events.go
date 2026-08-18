/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package events defines internal configuration event names and payload types.
//
// The configuration updated event is published to the system bus to
// synchronise configuration changes across connected RedisSMQ instances.
package events

import (
	"github.com/weyoss/go-redis-smq/pkg/config/cfg"
)

// EventUpdated is the name of the configuration updated event.
//
// It is internal-only and should not be subscribed to by external users.
const EventUpdated = "configuration.updated"

// UpdatedPayload contains the data for a configuration.updated event.
//
// It is used internally to deserialize the positional event arguments
// received from the event bus. The struct fields match the TypeScript
// event signature:
//
//	(config: IRedisSMQParsedConfig, version: number) => void
type UpdatedPayload struct {
	Config  *cfg.Config `json:"config"`
	Version int         `json:"version"`
}

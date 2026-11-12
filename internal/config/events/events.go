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
// The configuration updated event is published on the system bus to
// synchronise configuration changes across connected RedisSMQ instances.
package events

import (
	pubconfig "github.com/weyoss/go-redis-smq/pkg/config"
)

// EventUpdated is the name of the configuration updated event.
//
// It is internal-only and should not be subscribed to by external users.
const EventUpdated = "configuration.updated"

// UpdatedPayload contains the data for a configuration.updated event.
//
// Hybrid wire format:
//
//   - Go publishers emit 3 positional args (config, version, epoch).
//   - TypeScript publishers emit 2 positional args (config, version);
//     JS handler dispatch ignores extra args on the receiving side, so
//     a TS subscriber sees a well-formed 2-arg call even when the
//     publisher emitted 3.
//
// Epoch is populated when the event was published by a Go process; it
// is the empty string when the event was published by TypeScript. The
// subscriber dispatches on this to pick the right staleness-detection
// strategy — see config.Manager.subscribeToUpdatesLocked.
type UpdatedPayload struct {
	// Config is the full configuration that was saved.
	Config *pubconfig.Config `json:"config"`

	// Version is the configuration version after the update.
	Version int `json:"version"`

	// Epoch is the unique generation identifier of the configuration
	// record. Populated only when the event originated from a Go
	// publisher. Empty when the event originated from TypeScript.
	Epoch string `json:"epoch,omitempty"`
}

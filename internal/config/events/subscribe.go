/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package events

import (
	"encoding/json"

	"github.com/weyoss/go-redis-smq/internal/eventbus"
	pubconfig "github.com/weyoss/go-redis-smq/pkg/config"
)

// decodeArg converts a positional event argument (typically a
// map[string]interface{}) into the target Go type.
func decodeArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// SubscribeUpdated subscribes to configuration.updated events on the system bus.
//
// The handler receives an UpdatedPayload containing the new configuration,
// its version, and the epoch of the configuration record that generated the
// event.
//
// Hybrid wire format:
//
//   - Go publishers emit 3 positional args: (config, version, epoch).
//   - TypeScript publishers emit 2 positional args: (config, version) —
//     TS never generates an epoch. The subscriber accepts this form and
//     leaves UpdatedPayload.Epoch empty; the manager's handler dispatches
//     on Epoch == "" to select version-only staleness detection.
//
// A 3-arg Go publish and a 2-arg TS publish therefore both decode to a
// fully-populated UpdatedPayload. The only difference is whether Epoch
// carries a value.
func SubscribeUpdated(handler func(UpdatedPayload)) (*eventbus.Subscription, error) {
	return eventbus.System().Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}
		var config pubconfig.Config
		var version int
		if err := decodeArg(args[0], &config); err != nil {
			return
		}
		if err := decodeArg(args[1], &version); err != nil {
			return
		}

		// Third arg is optional. Its absence (TS publisher) leaves Epoch
		// empty, which the manager's handler treats as "no epoch to
		// compare — fall back to version-only dispatch."
		var epoch string
		if len(args) >= 3 {
			if err := decodeArg(args[2], &epoch); err != nil {
				return
			}
		}

		handler(UpdatedPayload{
			Config:  &config,
			Version: version,
			Epoch:   epoch,
		})
	}, EventUpdated)
}

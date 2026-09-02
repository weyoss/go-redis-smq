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
func SubscribeUpdated(handler func(UpdatedPayload)) (*eventbus.Subscription, error) {
	return eventbus.System().Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}
		var config pubconfig.Config
		var version int
		var epoch string
		if err := decodeArg(args[0], &config); err != nil {
			return
		}
		if err := decodeArg(args[1], &version); err != nil {
			return
		}
		if err := decodeArg(args[2], &epoch); err != nil {
			return
		}
		handler(UpdatedPayload{
			Config:  &config,
			Version: version,
			Epoch:   epoch,
		})
	}, EventUpdated)
}

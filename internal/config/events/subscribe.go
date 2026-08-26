/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package events provides internal subscription functions for RedisSMQ
// configuration events on the system event bus.
package events

import (
	"encoding/json"

	"github.com/weyoss/go-redis-smq/internal/eventbus"
	"github.com/weyoss/go-redis-smq/pkg/config"
)

func decodeArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// SubscribeUpdated subscribes to configuration.updated events on the system bus.
func SubscribeUpdated(handler func(UpdatedPayload)) (*eventbus.Subscription, error) {
	return eventbus.System().Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}
		var cfg config.Config
		var version int
		if err := decodeArg(args[0], &cfg); err != nil {
			return
		}
		if err := decodeArg(args[1], &version); err != nil {
			return
		}
		handler(UpdatedPayload{Config: &cfg, Version: version})
	}, EventUpdated)
}

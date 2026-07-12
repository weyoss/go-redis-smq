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
	"log"

	"github.com/weyoss/go-redis-smq/internal/config/events"
	"github.com/weyoss/go-redis-smq/internal/eventbus"
)

func SubscribeUpdated(handler func(payload events.UpdatedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.UpdatedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("config events: failed to unmarshal Updated payload: %v", err)
			return
		}
		handler(p)
	}, events.EventUpdated)
}

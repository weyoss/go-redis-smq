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

	"github.com/weyoss/go-redis-smq/internal/eventbus"
	"github.com/weyoss/go-redis-smq/internal/queue/events"
)

func SubscribeCreated(handler func(payload events.CreatedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.CreatedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("queue events: failed to unmarshal Created payload: %v", err)
			return
		}
		handler(p)
	}, events.EventCreated)
}

func SubscribeDeleted(handler func(events.DeletedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.DeletedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("queue events: failed to unmarshal Deleted payload: %v", err)
			return
		}
		handler(p)
	}, events.EventDeleted)
}

func SubscribeStateChanged(handler func(events.StateChangedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.StateChangedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("queue events: failed to unmarshal StateChanged payload: %v", err)
			return
		}
		handler(p)
	}, events.EventStateChanged)
}

func SubscribeConsumerGroupCreated(handler func(events.ConsumerGroupCreatedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.ConsumerGroupCreatedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("queue events: failed to unmarshal ConsumerGroupCreated payload: %v", err)
			return
		}
		handler(p)
	}, events.EventConsumerGroupCreated)
}

func SubscribeConsumerGroupDeleted(handler func(events.ConsumerGroupDeletedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.ConsumerGroupDeletedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("queue events: failed to unmarshal ConsumerGroupDeleted payload: %v", err)
			return
		}
		handler(p)
	}, events.EventConsumerGroupDeleted)
}

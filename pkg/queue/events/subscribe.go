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
	internalEvents "github.com/weyoss/go-redis-smq/internal/queue/events"
)

// Re-export internal payload types so external users can refer to them
// without importing internal packages.

type CreatedPayload = internalEvents.CreatedPayload
type DeletedPayload = internalEvents.DeletedPayload
type StateChangedPayload = internalEvents.StateChangedPayload
type ConsumerGroupCreatedPayload = internalEvents.ConsumerGroupCreatedPayload
type ConsumerGroupDeletedPayload = internalEvents.ConsumerGroupDeletedPayload

func SubscribeCreated(handler func(payload CreatedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p CreatedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("queue events: failed to unmarshal Created payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventCreated)
}

func SubscribeDeleted(handler func(DeletedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p DeletedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("queue events: failed to unmarshal Deleted payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventDeleted)
}

func SubscribeStateChanged(handler func(StateChangedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p StateChangedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("queue events: failed to unmarshal StateChanged payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventStateChanged)
}

func SubscribeConsumerGroupCreated(handler func(ConsumerGroupCreatedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p ConsumerGroupCreatedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("queue events: failed to unmarshal ConsumerGroupCreated payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventConsumerGroupCreated)
}

func SubscribeConsumerGroupDeleted(handler func(ConsumerGroupDeletedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p ConsumerGroupDeletedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("queue events: failed to unmarshal ConsumerGroupDeleted payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventConsumerGroupDeleted)
}

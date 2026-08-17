/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package events provides public subscription functions for RedisSMQ queue events.
//
// The functions in this package let external consumers observe queue lifecycle
// events such as creation, deletion, state changes, and consumer group changes.
package events

import (
	"encoding/json"
	"log"

	"github.com/weyoss/go-redis-smq/internal/eventbus"
	internalEvents "github.com/weyoss/go-redis-smq/internal/queue/events"
)

// Re-exported payload types. These aliases allow external users to refer to
// event payload types without importing internal packages.

// CreatedPayload is the payload for the queue.queueCreated event.
type CreatedPayload = internalEvents.CreatedPayload

// DeletedPayload is the payload for the queue.queueDeleted event.
type DeletedPayload = internalEvents.DeletedPayload

// StateChangedPayload is the payload for the queue.stateChanged event.
type StateChangedPayload = internalEvents.StateChangedPayload

// ConsumerGroupCreatedPayload is the payload for the
// queue.consumerGroupCreated event.
type ConsumerGroupCreatedPayload = internalEvents.ConsumerGroupCreatedPayload

// ConsumerGroupDeletedPayload is the payload for the
// queue.consumerGroupDeleted event.
type ConsumerGroupDeletedPayload = internalEvents.ConsumerGroupDeletedPayload

// SubscribeCreated registers a handler for the queue.queueCreated event.
//
// The handler receives a CreatedPayload containing the queue and its
// properties. The returned subscription can be used to unsubscribe.
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

// SubscribeDeleted registers a handler for the queue.queueDeleted event.
//
// The handler receives a DeletedPayload containing the deleted queue.
// The returned subscription can be used to unsubscribe.
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

// SubscribeStateChanged registers a handler for the queue.stateChanged event.
//
// The handler receives a StateChangedPayload containing the queue and the
// state transition. The returned subscription can be used to unsubscribe.
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

// SubscribeConsumerGroupCreated registers a handler for the
// queue.consumerGroupCreated event.
//
// The handler receives a ConsumerGroupCreatedPayload containing the queue
// and the new group ID.
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

// SubscribeConsumerGroupDeleted registers a handler for the
// queue.consumerGroupDeleted event.
//
// The handler receives a ConsumerGroupDeletedPayload containing the queue
// and the deleted group ID.
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

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package events provides public subscription functions for RedisSMQ producer events.
//
// These functions allow external users to subscribe to producer lifecycle
// events and message publication events without importing internal packages.
package events

import (
	"encoding/json"
	"log"

	"github.com/weyoss/go-redis-smq/internal/eventbus"
	internalEvents "github.com/weyoss/go-redis-smq/internal/producer/events"
)

// Re-exported payload types. These aliases allow external users to refer to
// event payload types without importing internal packages.

// LifecyclePayload is the payload for producer lifecycle events such as
// producer.up, producer.down, producer.goingUp, and producer.goingDown.
type LifecyclePayload = internalEvents.LifecyclePayload

// MessagePublishedPayload is the payload for the producer.messagePublished
// event.
type MessagePublishedPayload = internalEvents.MessagePublishedPayload

// SubscribeUp registers a handler for the producer.up event.
//
// The handler receives a LifecyclePayload containing the producer ID.
func SubscribeUp(handler func(payload LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("producer events: failed to unmarshal Up payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventUp)
}

// SubscribeDown registers a handler for the producer.down event.
//
// The handler receives a LifecyclePayload containing the producer ID.
func SubscribeDown(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("producer events: failed to unmarshal Down payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventDown)
}

// SubscribeGoingUp registers a handler for the producer.goingUp event.
//
// The handler receives a LifecyclePayload containing the producer ID.
func SubscribeGoingUp(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("producer events: failed to unmarshal GoingUp payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventGoingUp)
}

// SubscribeGoingDown registers a handler for the producer.goingDown event.
//
// The handler receives a LifecyclePayload containing the producer ID.
func SubscribeGoingDown(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("producer events: failed to unmarshal GoingDown payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventGoingDown)
}

// SubscribeMessagePublished registers a handler for the
// producer.messagePublished event.
//
// The handler receives a MessagePublishedPayload containing the message ID,
// destination queue, producer ID, and optional consumer group ID.
func SubscribeMessagePublished(handler func(MessagePublishedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p MessagePublishedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("producer events: failed to unmarshal MessagePublished payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventMessagePublished)
}

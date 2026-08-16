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
	internalEvents "github.com/weyoss/go-redis-smq/internal/producer/events"
)

// Re-export internal payload types so external users can refer to them
// without importing internal packages.

type LifecyclePayload = internalEvents.LifecyclePayload
type MessagePublishedPayload = internalEvents.MessagePublishedPayload

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

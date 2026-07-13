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
	"github.com/weyoss/go-redis-smq/internal/producer/events"
)

func SubscribeUp(handler func(payload events.LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("producer events: failed to unmarshal Up payload: %v", err)
			return
		}
		handler(p)
	}, events.EventUp)
}

func SubscribeDown(handler func(events.LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("producer events: failed to unmarshal Down payload: %v", err)
			return
		}
		handler(p)
	}, events.EventDown)
}

func SubscribeGoingUp(handler func(events.LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("producer events: failed to unmarshal GoingUp payload: %v", err)
			return
		}
		handler(p)
	}, events.EventGoingUp)
}

func SubscribeGoingDown(handler func(events.LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("producer events: failed to unmarshal GoingDown payload: %v", err)
			return
		}
		handler(p)
	}, events.EventGoingDown)
}

func SubscribeMessagePublished(handler func(events.MessagePublishedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.MessagePublishedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("producer events: failed to unmarshal MessagePublished payload: %v", err)
			return
		}
		handler(p)
	}, events.EventMessagePublished)
}

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

	"github.com/weyoss/go-redis-smq/internal/consumer/events"
	"github.com/weyoss/go-redis-smq/internal/eventbus"
)

func SubscribeUp(handler func(payload events.LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal Up payload: %v", err)
			return
		}
		handler(p)
	}, events.EventUp)
}

func SubscribeDown(handler func(events.LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal Down payload: %v", err)
			return
		}
		handler(p)
	}, events.EventDown)
}

func SubscribeGoingUp(handler func(events.LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal GoingUp payload: %v", err)
			return
		}
		handler(p)
	}, events.EventGoingUp)
}

func SubscribeGoingDown(handler func(events.LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal GoingDown payload: %v", err)
			return
		}
		handler(p)
	}, events.EventGoingDown)
}

func SubscribeMessageReceived(handler func(events.MessageReceivedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.MessageReceivedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageReceived payload: %v", err)
			return
		}
		handler(p)
	}, events.EventMessageReceived)
}

func SubscribeMessageAcknowledged(handler func(events.MessagePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.MessagePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageAcknowledged payload: %v", err)
			return
		}
		handler(p)
	}, events.EventMessageAcknowledged)
}

func SubscribeMessageUnacknowledged(handler func(events.MessageUnacknowledgedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.MessageUnacknowledgedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageUnacknowledged payload: %v", err)
			return
		}
		handler(p)
	}, events.EventMessageUnacknowledged)
}

func SubscribeMessageDeadLettered(handler func(events.MessageDeadLetteredPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.MessageDeadLetteredPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageDeadLettered payload: %v", err)
			return
		}
		handler(p)
	}, events.EventMessageDeadLettered)
}

func SubscribeMessageRequeued(handler func(events.MessagePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.MessagePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageRequeued payload: %v", err)
			return
		}
		handler(p)
	}, events.EventMessageRequeued)
}

func SubscribeMessageDelayed(handler func(events.MessagePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p events.MessagePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageDelayed payload: %v", err)
			return
		}
		handler(p)
	}, events.EventMessageDelayed)
}

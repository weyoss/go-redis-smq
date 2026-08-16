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

	internalEvents "github.com/weyoss/go-redis-smq/internal/consumer/events"
	"github.com/weyoss/go-redis-smq/internal/eventbus"
)

// Re-export internal payload types so external users can refer to them
// without importing internal packages.

type LifecyclePayload = internalEvents.LifecyclePayload
type MessagePayload = internalEvents.MessagePayload
type MessageUnacknowledgedPayload = internalEvents.MessageUnacknowledgedPayload
type MessageDeadLetteredPayload = internalEvents.MessageDeadLetteredPayload
type MessageReceivedPayload = internalEvents.MessageReceivedPayload

func SubscribeUp(handler func(payload LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal Up payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventUp)
}

func SubscribeDown(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal Down payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventDown)
}

func SubscribeGoingUp(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal GoingUp payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventGoingUp)
}

func SubscribeGoingDown(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p LifecyclePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal GoingDown payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventGoingDown)
}

func SubscribeMessageReceived(handler func(MessageReceivedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p MessageReceivedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageReceived payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventMessageReceived)
}

func SubscribeMessageAcknowledged(handler func(MessagePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p MessagePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageAcknowledged payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventMessageAcknowledged)
}

func SubscribeMessageUnacknowledged(handler func(MessageUnacknowledgedPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p MessageUnacknowledgedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageUnacknowledged payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventMessageUnacknowledged)
}

func SubscribeMessageDeadLettered(handler func(MessageDeadLetteredPayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p MessageDeadLetteredPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageDeadLettered payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventMessageDeadLettered)
}

func SubscribeMessageRequeued(handler func(MessagePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p MessagePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageRequeued payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventMessageRequeued)
}

func SubscribeMessageDelayed(handler func(MessagePayload)) (*eventbus.Subscription, error) {
	return eventbus.Singleton().Subscribe(func(_ string, payload json.RawMessage) {
		var p MessagePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("consumer events: failed to unmarshal MessageDelayed payload: %v", err)
			return
		}
		handler(p)
	}, internalEvents.EventMessageDelayed)
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer

import (
	"encoding/json"
	"fmt"

	"github.com/weyoss/go-redis-smq/pkg/eventbus"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Public event names.
const (
	EventUp               = "producer.up"
	EventDown             = "producer.down"
	EventGoingUp          = "producer.goingUp"
	EventGoingDown        = "producer.goingDown"
	EventMessagePublished = "producer.messagePublished"
)

type LifecyclePayload struct {
	ProducerID string
}

// MessagePublishedPayload is the payload for producer.messagePublished
// events.
//
// Queue carries both the queue's (name, ns) identity and the effective
// consumer group ID.
// For point-to-point queues, Queue.GroupID is empty.
type MessagePublishedPayload struct {
	MessageID  string
	Queue      queue.ParsedParams
	ProducerID string
}

func decodeArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// decodeQueueArg decodes the queue argument from an event payload into a
// queue.ParsedParams. Two wire formats are accepted:
//
//  1. The current format, matching TypeScript's IQueueParsedParams:
//     {"queueParams":{"name":"...","ns":"..."},"groupId":"..."}
//
//  2. The legacy format emitted by older Go versions:
//     {"name":"...","ns":"..."}
//
// The current format is tried first; if the decoded QueueParams has an
// empty name (meaning the input didn't have a nested "queueParams" field
// — Params.UnmarshalJSON rejects empty names so a successful decode
// always has a non-empty name), the legacy path is used. This makes
// two-phase rolling upgrades possible: deploy the subscriber update
// first (accepts both), then deploy the publisher update (emits only the
// new format).
func decodeQueueArg(arg interface{}, target *queue.ParsedParams) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}

	var pp queue.ParsedParams
	if err := json.Unmarshal(data, &pp); err == nil && pp.QueueParams.Name() != "" {
		*target = pp
		return nil
	}

	var legacy queue.Params
	if err := json.Unmarshal(data, &legacy); err == nil && legacy.Name() != "" {
		*target = queue.ParsedParams{QueueParams: legacy, GroupID: ""}
		return nil
	}

	return fmt.Errorf("decode queue: unrecognized format")
}

// SubscribeUp registers a handler for the producer.up event.
func SubscribeUp(handler func(LifecyclePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("producer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var producerID string
		if err := decodeArg(args[0], &producerID); err != nil {
			return
		}
		handler(LifecyclePayload{ProducerID: producerID})
	}, EventUp)
}

// SubscribeDown registers a handler for the producer.down event.
func SubscribeDown(handler func(LifecyclePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("producer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var producerID string
		if err := decodeArg(args[0], &producerID); err != nil {
			return
		}
		handler(LifecyclePayload{ProducerID: producerID})
	}, EventDown)
}

// SubscribeGoingUp registers a handler for the producer.goingUp event.
func SubscribeGoingUp(handler func(LifecyclePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("producer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var producerID string
		if err := decodeArg(args[0], &producerID); err != nil {
			return
		}
		handler(LifecyclePayload{ProducerID: producerID})
	}, EventGoingUp)
}

// SubscribeGoingDown registers a handler for the producer.goingDown event.
func SubscribeGoingDown(handler func(LifecyclePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("producer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var producerID string
		if err := decodeArg(args[0], &producerID); err != nil {
			return
		}
		handler(LifecyclePayload{ProducerID: producerID})
	}, EventGoingDown)
}

// SubscribeMessagePublished registers a handler for the
// producer.messagePublished event.
func SubscribeMessagePublished(handler func(MessagePublishedPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("producer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}

		var messageID string
		var queueParsed queue.ParsedParams
		var producerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeQueueArg(args[1], &queueParsed); err != nil {
			return
		}
		if err := decodeArg(args[2], &producerID); err != nil {
			return
		}

		handler(MessagePublishedPayload{
			MessageID:  messageID,
			Queue:      queueParsed,
			ProducerID: producerID,
		})
	}, EventMessagePublished)
}

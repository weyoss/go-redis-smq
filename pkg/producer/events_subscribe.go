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

// Public payload types.
type LifecyclePayload struct {
	ProducerID string
}

type MessagePublishedPayload struct {
	MessageID  string
	Queue      queue.Params
	ProducerID string
}

func decodeArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
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
		var queueParams queue.Params
		var producerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeArg(args[1], &queueParams); err != nil {
			return
		}
		if err := decodeArg(args[2], &producerID); err != nil {
			return
		}

		handler(MessagePublishedPayload{
			MessageID:  messageID,
			Queue:      queueParams,
			ProducerID: producerID,
		})
	}, EventMessagePublished)
}

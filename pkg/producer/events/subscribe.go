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
	"context"
	"encoding/json"

	"github.com/weyoss/go-redis-smq/internal/eventbus"
	internalEvents "github.com/weyoss/go-redis-smq/internal/producer/events"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Re-exported payload types. These aliases allow external users to refer to
// event payload types without importing internal packages.

// LifecyclePayload is the payload for producer lifecycle events such as
// producer.up, producer.down, producer.goingUp, and producer.goingDown.
type LifecyclePayload = internalEvents.LifecyclePayload

// MessagePublishedPayload is the payload for the producer.messagePublished
// event.
type MessagePublishedPayload = internalEvents.MessagePublishedPayload

func decodeArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// SubscribeUp registers a handler for the producer.up event.
func SubscribeUp(handler func(payload LifecyclePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var producerID string
		if err := decodeArg(args[0], &producerID); err != nil {
			return
		}
		handler(LifecyclePayload{ProducerID: producerID})
	}, internalEvents.EventUp)
}

// SubscribeDown registers a handler for the producer.down event.
func SubscribeDown(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var producerID string
		if err := decodeArg(args[0], &producerID); err != nil {
			return
		}
		handler(LifecyclePayload{ProducerID: producerID})
	}, internalEvents.EventDown)
}

// SubscribeGoingUp registers a handler for the producer.goingUp event.
func SubscribeGoingUp(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var producerID string
		if err := decodeArg(args[0], &producerID); err != nil {
			return
		}
		handler(LifecyclePayload{ProducerID: producerID})
	}, internalEvents.EventGoingUp)
}

// SubscribeGoingDown registers a handler for the producer.goingDown event.
func SubscribeGoingDown(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var producerID string
		if err := decodeArg(args[0], &producerID); err != nil {
			return
		}
		handler(LifecyclePayload{ProducerID: producerID})
	}, internalEvents.EventGoingDown)
}

// SubscribeMessagePublished registers a handler for the
// producer.messagePublished event.
//
// The handler receives a MessagePublishedPayload containing the message ID,
// destination queue, and producer ID.
func SubscribeMessagePublished(handler func(MessagePublishedPayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}

		var messageID string
		var queue q.QueueParams
		var producerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeArg(args[1], &queue); err != nil {
			return
		}
		if err := decodeArg(args[2], &producerID); err != nil {
			return
		}

		handler(MessagePublishedPayload{
			MessageID:  messageID,
			Queue:      queue,
			ProducerID: producerID,
		})
	}, internalEvents.EventMessagePublished)
}

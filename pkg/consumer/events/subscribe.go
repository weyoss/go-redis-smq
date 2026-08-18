/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package events provides public subscription functions for RedisSMQ consumer events.
//
// These functions automatically start the public user event bus on first use.
// No explicit initialisation is required.
package events

import (
	"context"
	"encoding/json"

	internalEvents "github.com/weyoss/go-redis-smq/internal/consumer/events"
	"github.com/weyoss/go-redis-smq/internal/eventbus"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Re-exported payload types. These aliases allow external users to refer to
// event payload types without importing internal packages.

// LifecyclePayload is the payload for consumer lifecycle events such as
// consumer.up, consumer.down, consumer.goingUp, and consumer.goingDown.
type LifecyclePayload = internalEvents.LifecyclePayload

// MessagePayload is the base payload for consumer message events like
// consumer.messageAcknowledged, consumer.messageRequeued, and
// consumer.messageDelayed.
type MessagePayload = internalEvents.MessagePayload

// MessageUnacknowledgedPayload is the payload for the
// consumer.messageUnacknowledged event.
type MessageUnacknowledgedPayload = internalEvents.MessageUnacknowledgedPayload

// MessageDeadLetteredPayload is the payload for the
// consumer.messageDeadLettered event.
type MessageDeadLetteredPayload = internalEvents.MessageDeadLetteredPayload

// MessageReceivedPayload is the payload for the consumer.messageReceived
// event.
type MessageReceivedPayload = internalEvents.MessageReceivedPayload

func decodeArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// SubscribeUp registers a handler for the consumer.up event.
func SubscribeUp(handler func(payload LifecyclePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var consumerID string
		if err := decodeArg(args[0], &consumerID); err != nil {
			return
		}
		handler(LifecyclePayload{ConsumerID: consumerID})
	}, internalEvents.EventUp)
}

// SubscribeDown registers a handler for the consumer.down event.
func SubscribeDown(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var consumerID string
		if err := decodeArg(args[0], &consumerID); err != nil {
			return
		}
		handler(LifecyclePayload{ConsumerID: consumerID})
	}, internalEvents.EventDown)
}

// SubscribeGoingUp registers a handler for the consumer.goingUp event.
func SubscribeGoingUp(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var consumerID string
		if err := decodeArg(args[0], &consumerID); err != nil {
			return
		}
		handler(LifecyclePayload{ConsumerID: consumerID})
	}, internalEvents.EventGoingUp)
}

// SubscribeGoingDown registers a handler for the consumer.goingDown event.
func SubscribeGoingDown(handler func(LifecyclePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var consumerID string
		if err := decodeArg(args[0], &consumerID); err != nil {
			return
		}
		handler(LifecyclePayload{ConsumerID: consumerID})
	}, internalEvents.EventGoingDown)
}

// SubscribeMessageReceived registers a handler for the
// consumer.messageReceived event.
func SubscribeMessageReceived(handler func(MessageReceivedPayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}

		var messageID string
		var queue q.QueueParams
		var consumerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeArg(args[1], &queue); err != nil {
			return
		}
		if err := decodeArg(args[2], &consumerID); err != nil {
			return
		}

		handler(MessageReceivedPayload{MessageID: messageID, Queue: queue, ConsumerID: consumerID})
	}, internalEvents.EventMessageReceived)
}

// SubscribeMessageAcknowledged registers a handler for the
// consumer.messageAcknowledged event.
//
// The event payload contains three positional arguments:
// messageId, queue, consumerId.
func SubscribeMessageAcknowledged(handler func(MessagePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}

		var messageID string
		var queue q.QueueParams
		var consumerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeArg(args[1], &queue); err != nil {
			return
		}
		if err := decodeArg(args[2], &consumerID); err != nil {
			return
		}

		handler(MessagePayload{
			MessageID:  messageID,
			Queue:      queue,
			ConsumerID: consumerID,
		})
	}, internalEvents.EventMessageAcknowledged)
}

// SubscribeMessageUnacknowledged registers a handler for the
// consumer.messageUnacknowledged event.
//
// The event payload contains four positional arguments:
// messageId, queue, consumerId, cause.
func SubscribeMessageUnacknowledged(handler func(MessageUnacknowledgedPayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 4 {
			return
		}

		var messageID string
		var queue q.QueueParams
		var consumerID string
		var cause int

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeArg(args[1], &queue); err != nil {
			return
		}
		if err := decodeArg(args[2], &consumerID); err != nil {
			return
		}
		if err := decodeArg(args[3], &cause); err != nil {
			return
		}

		handler(MessageUnacknowledgedPayload{
			MessagePayload: MessagePayload{
				MessageID:  messageID,
				Queue:      queue,
				ConsumerID: consumerID,
			},
			Cause: cause,
		})
	}, internalEvents.EventMessageUnacknowledged)
}

// SubscribeMessageDeadLettered registers a handler for the
// consumer.messageDeadLettered event.
//
// The event payload contains four positional arguments:
// messageId, queue, consumerId, cause.
func SubscribeMessageDeadLettered(handler func(MessageDeadLetteredPayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 4 {
			return
		}

		var messageID string
		var queue q.QueueParams
		var consumerID string
		var cause int

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeArg(args[1], &queue); err != nil {
			return
		}
		if err := decodeArg(args[2], &consumerID); err != nil {
			return
		}
		if err := decodeArg(args[3], &cause); err != nil {
			return
		}

		handler(MessageDeadLetteredPayload{
			MessagePayload: MessagePayload{
				MessageID:  messageID,
				Queue:      queue,
				ConsumerID: consumerID,
			},
			Cause: cause,
		})
	}, internalEvents.EventMessageDeadLettered)
}

// SubscribeMessageRequeued registers a handler for the
// consumer.messageRequeued event.
//
// The event payload contains three positional arguments:
// messageId, queue, consumerId.
func SubscribeMessageRequeued(handler func(MessagePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}

		var messageID string
		var queue q.QueueParams
		var consumerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeArg(args[1], &queue); err != nil {
			return
		}
		if err := decodeArg(args[2], &consumerID); err != nil {
			return
		}

		handler(MessagePayload{
			MessageID:  messageID,
			Queue:      queue,
			ConsumerID: consumerID,
		})
	}, internalEvents.EventMessageRequeued)
}

// SubscribeMessageDelayed registers a handler for the
// consumer.messageDelayed event.
//
// The event payload contains three positional arguments:
// messageId, queue, consumerId.
func SubscribeMessageDelayed(handler func(MessagePayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}

		var messageID string
		var queue q.QueueParams
		var consumerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeArg(args[1], &queue); err != nil {
			return
		}
		if err := decodeArg(args[2], &consumerID); err != nil {
			return
		}

		handler(MessagePayload{
			MessageID:  messageID,
			Queue:      queue,
			ConsumerID: consumerID,
		})
	}, internalEvents.EventMessageDelayed)
}

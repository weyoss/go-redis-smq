/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer

import (
	"encoding/json"
	"fmt"

	"github.com/weyoss/go-redis-smq/pkg/eventbus"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

const (
	EventUp                    = "consumer.up"
	EventDown                  = "consumer.down"
	EventGoingUp               = "consumer.goingUp"
	EventGoingDown             = "consumer.goingDown"
	EventMessageReceived       = "consumer.messageReceived"
	EventMessageAcknowledged   = "consumer.messageAcknowledged"
	EventMessageUnacknowledged = "consumer.messageUnacknowledged"
	EventMessageDeadLettered   = "consumer.messageDeadLettered"
	EventMessageRequeued       = "consumer.messageRequeued"
	EventMessageDelayed        = "consumer.messageDelayed"
)

type LifecyclePayload struct {
	ConsumerID string
}

// MessagePayload is the base payload for consumer message events.
//
// Queue carries both the queue's (name, ns) identity and the effective
// consumer group ID.
// For point-to-point queues, Queue.GroupID is empty.
type MessagePayload struct {
	MessageID  string
	Queue      queue.ParsedParams
	ConsumerID string
}

type MessageUnacknowledgedPayload struct {
	MessagePayload
	Cause int
}

type MessageDeadLetteredPayload struct {
	MessagePayload
	Cause int
}

type MessageReceivedPayload struct {
	MessageID  string
	Queue      queue.ParsedParams
	ConsumerID string
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

// SubscribeUp registers a handler for the consumer.up event.
func SubscribeUp(handler func(LifecyclePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("consumer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var consumerID string
		if err := decodeArg(args[0], &consumerID); err != nil {
			return
		}
		handler(LifecyclePayload{ConsumerID: consumerID})
	}, EventUp)
}

// SubscribeDown registers a handler for the consumer.down event.
func SubscribeDown(handler func(LifecyclePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("consumer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var consumerID string
		if err := decodeArg(args[0], &consumerID); err != nil {
			return
		}
		handler(LifecyclePayload{ConsumerID: consumerID})
	}, EventDown)
}

// SubscribeGoingUp registers a handler for the consumer.goingUp event.
func SubscribeGoingUp(handler func(LifecyclePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("consumer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var consumerID string
		if err := decodeArg(args[0], &consumerID); err != nil {
			return
		}
		handler(LifecyclePayload{ConsumerID: consumerID})
	}, EventGoingUp)
}

// SubscribeGoingDown registers a handler for the consumer.goingDown event.
func SubscribeGoingDown(handler func(LifecyclePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("consumer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var consumerID string
		if err := decodeArg(args[0], &consumerID); err != nil {
			return
		}
		handler(LifecyclePayload{ConsumerID: consumerID})
	}, EventGoingDown)
}

// SubscribeMessageReceived registers a handler for the
// consumer.messageReceived event.
func SubscribeMessageReceived(handler func(MessageReceivedPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("consumer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}

		var messageID string
		var queueParsed queue.ParsedParams
		var consumerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeQueueArg(args[1], &queueParsed); err != nil {
			return
		}
		if err := decodeArg(args[2], &consumerID); err != nil {
			return
		}

		handler(MessageReceivedPayload{MessageID: messageID, Queue: queueParsed, ConsumerID: consumerID})
	}, EventMessageReceived)
}

// SubscribeMessageAcknowledged registers a handler for the
// consumer.messageAcknowledged event.
func SubscribeMessageAcknowledged(handler func(MessagePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("consumer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}

		var messageID string
		var queueParsed queue.ParsedParams
		var consumerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeQueueArg(args[1], &queueParsed); err != nil {
			return
		}
		if err := decodeArg(args[2], &consumerID); err != nil {
			return
		}

		handler(MessagePayload{
			MessageID:  messageID,
			Queue:      queueParsed,
			ConsumerID: consumerID,
		})
	}, EventMessageAcknowledged)
}

// SubscribeMessageUnacknowledged registers a handler for the
// consumer.messageUnacknowledged event.
func SubscribeMessageUnacknowledged(handler func(MessageUnacknowledgedPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("consumer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 4 {
			return
		}

		var messageID string
		var queueParsed queue.ParsedParams
		var consumerID string
		var cause int

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeQueueArg(args[1], &queueParsed); err != nil {
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
				Queue:      queueParsed,
				ConsumerID: consumerID,
			},
			Cause: cause,
		})
	}, EventMessageUnacknowledged)
}

// SubscribeMessageDeadLettered registers a handler for the
// consumer.messageDeadLettered event.
func SubscribeMessageDeadLettered(handler func(MessageDeadLetteredPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("consumer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 4 {
			return
		}

		var messageID string
		var queueParsed queue.ParsedParams
		var consumerID string
		var cause int

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeQueueArg(args[1], &queueParsed); err != nil {
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
				Queue:      queueParsed,
				ConsumerID: consumerID,
			},
			Cause: cause,
		})
	}, EventMessageDeadLettered)
}

// SubscribeMessageRequeued registers a handler for the
// consumer.messageRequeued event.
func SubscribeMessageRequeued(handler func(MessagePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("consumer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}

		var messageID string
		var queueParsed queue.ParsedParams
		var consumerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeQueueArg(args[1], &queueParsed); err != nil {
			return
		}
		if err := decodeArg(args[2], &consumerID); err != nil {
			return
		}

		handler(MessagePayload{
			MessageID:  messageID,
			Queue:      queueParsed,
			ConsumerID: consumerID,
		})
	}, EventMessageRequeued)
}

// SubscribeMessageDelayed registers a handler for the
// consumer.messageDelayed event.
func SubscribeMessageDelayed(handler func(MessagePayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("consumer events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 3 {
			return
		}

		var messageID string
		var queueParsed queue.ParsedParams
		var consumerID string

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeQueueArg(args[1], &queueParsed); err != nil {
			return
		}
		if err := decodeArg(args[2], &consumerID); err != nil {
			return
		}

		handler(MessagePayload{
			MessageID:  messageID,
			Queue:      queueParsed,
			ConsumerID: consumerID,
		})
	}, EventMessageDelayed)
}

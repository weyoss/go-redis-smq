/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue

import (
	"encoding/json"
	"fmt"

	"github.com/weyoss/go-redis-smq/pkg/eventbus"
)

// Public event names.
const (
	EventCreated              = "queue.queueCreated"
	EventDeleted              = "queue.queueDeleted"
	EventStateChanged         = "queue.stateChanged"
	EventConsumerGroupCreated = "queue.consumerGroupCreated"
	EventConsumerGroupDeleted = "queue.consumerGroupDeleted"
)

// CreatedPayload is the payload for the queue.queueCreated event.
type CreatedPayload struct {
	Queue      QueueParams
	Properties QueueProps
}

// DeletedPayload is the payload for the queue.queueDeleted event.
type DeletedPayload struct {
	Queue QueueParams
}

// StateChangedPayload is the payload for the queue.stateChanged event.
type StateChangedPayload struct {
	Queue      QueueParams
	Transition StateTransition
}

// ConsumerGroupCreatedPayload is the payload for the
// queue.consumerGroupCreated event.
type ConsumerGroupCreatedPayload struct {
	Queue   QueueParams
	GroupID string
}

// ConsumerGroupDeletedPayload is the payload for the
// queue.consumerGroupDeleted event.
type ConsumerGroupDeletedPayload struct {
	Queue   QueueParams
	GroupID string
}

func decodeArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// SubscribeCreated registers a handler for the queue.queueCreated event.
func SubscribeCreated(handler func(CreatedPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("queue events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}
		var queue QueueParams
		var props QueueProps
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		if err := decodeArg(args[1], &props); err != nil {
			return
		}
		handler(CreatedPayload{Queue: queue, Properties: props})
	}, EventCreated)
}

// SubscribeDeleted registers a handler for the queue.queueDeleted event.
func SubscribeDeleted(handler func(DeletedPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("queue events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var queue QueueParams
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		handler(DeletedPayload{Queue: queue})
	}, EventDeleted)
}

// SubscribeStateChanged registers a handler for the queue.stateChanged event.
func SubscribeStateChanged(handler func(StateChangedPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("queue events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}
		var queue QueueParams
		var transition StateTransition
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		if err := decodeArg(args[1], &transition); err != nil {
			return
		}
		handler(StateChangedPayload{Queue: queue, Transition: transition})
	}, EventStateChanged)
}

// SubscribeConsumerGroupCreated registers a handler for the
// queue.consumerGroupCreated event.
func SubscribeConsumerGroupCreated(handler func(ConsumerGroupCreatedPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("queue events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}
		var queue QueueParams
		var groupID string
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		if err := decodeArg(args[1], &groupID); err != nil {
			return
		}
		handler(ConsumerGroupCreatedPayload{Queue: queue, GroupID: groupID})
	}, EventConsumerGroupCreated)
}

// SubscribeConsumerGroupDeleted registers a handler for the
// queue.consumerGroupDeleted event.
func SubscribeConsumerGroupDeleted(handler func(ConsumerGroupDeletedPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("queue events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}
		var queue QueueParams
		var groupID string
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		if err := decodeArg(args[1], &groupID); err != nil {
			return
		}
		handler(ConsumerGroupDeletedPayload{Queue: queue, GroupID: groupID})
	}, EventConsumerGroupDeleted)
}

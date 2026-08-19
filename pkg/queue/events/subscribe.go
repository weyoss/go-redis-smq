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
	internalEvents "github.com/weyoss/go-redis-smq/internal/queue/events"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Re-exported payload types. These aliases allow external users to refer to
// event payload types without importing internal packages.

// CreatedPayload is the payload for the queue.queueCreated event.
type CreatedPayload = internalEvents.CreatedPayload

// DeletedPayload is the payload for the queue.queueDeleted event.
type DeletedPayload = internalEvents.DeletedPayload

// StateChangedPayload is the payload for the queue.stateChanged event.
type StateChangedPayload = internalEvents.StateChangedPayload

// ConsumerGroupCreatedPayload is the payload for the
// queue.consumerGroupCreated event.
type ConsumerGroupCreatedPayload = internalEvents.ConsumerGroupCreatedPayload

// ConsumerGroupDeletedPayload is the payload for the
// queue.consumerGroupDeleted event.
type ConsumerGroupDeletedPayload = internalEvents.ConsumerGroupDeletedPayload

func decodeArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// SubscribeCreated registers a handler for the queue.queueCreated event.
//
// The handler receives a CreatedPayload containing the queue and its
// properties. The returned subscription can be used to unsubscribe.
func SubscribeCreated(handler func(CreatedPayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}

		var queue q.QueueParams
		var props q.QueueProps
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		if err := decodeArg(args[1], &props); err != nil {
			return
		}

		handler(CreatedPayload{Queue: queue, Properties: props})
	}, internalEvents.EventCreated)
}

// SubscribeDeleted registers a handler for the queue.queueDeleted event.
//
// The handler receives a DeletedPayload containing the deleted queue.
func SubscribeDeleted(handler func(DeletedPayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}

		var queue q.QueueParams
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}

		handler(DeletedPayload{Queue: queue})
	}, internalEvents.EventDeleted)
}

// SubscribeStateChanged registers a handler for the queue.stateChanged event.
//
// The handler receives a StateChangedPayload containing the queue and the
// state transition.
func SubscribeStateChanged(handler func(StateChangedPayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}

		var queue q.QueueParams
		var transition q.StateTransition
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		if err := decodeArg(args[1], &transition); err != nil {
			return
		}

		handler(StateChangedPayload{Queue: queue, Transition: transition})
	}, internalEvents.EventStateChanged)
}

// SubscribeConsumerGroupCreated registers a handler for the
// queue.consumerGroupCreated event.
//
// The handler receives a ConsumerGroupCreatedPayload containing the queue
// and the new group ID.
func SubscribeConsumerGroupCreated(handler func(ConsumerGroupCreatedPayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}

		var queue q.QueueParams
		var groupID string
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		if err := decodeArg(args[1], &groupID); err != nil {
			return
		}

		handler(ConsumerGroupCreatedPayload{Queue: queue, GroupID: groupID})
	}, internalEvents.EventConsumerGroupCreated)
}

// SubscribeConsumerGroupDeleted registers a handler for the
// queue.consumerGroupDeleted event.
//
// The handler receives a ConsumerGroupDeletedPayload containing the queue
// and the deleted group ID.
func SubscribeConsumerGroupDeleted(handler func(ConsumerGroupDeletedPayload)) (*eventbus.Subscription, error) {
	bus := eventbus.InitUser(context.Background())

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}

		var queue q.QueueParams
		var groupID string
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		if err := decodeArg(args[1], &groupID); err != nil {
			return
		}

		handler(ConsumerGroupDeletedPayload{Queue: queue, GroupID: groupID})
	}, internalEvents.EventConsumerGroupDeleted)
}

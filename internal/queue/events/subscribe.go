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

	"github.com/weyoss/go-redis-smq/internal/eventbus"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// decodeArg converts a positional event argument (typically a
// map[string]interface{}) into the target Go type.
func decodeArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// SubscribeCreated subscribes to queue.queueCreated events on the system bus.
// The handler receives a CreatedPayload.
func SubscribeCreated(handler func(CreatedPayload)) (*eventbus.Subscription, error) {
	return eventbus.System().Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}
		var queue publicqueue.Params
		var props publicqueue.Props
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		if err := decodeArg(args[1], &props); err != nil {
			return
		}
		handler(CreatedPayload{Queue: queue, Properties: props})
	}, EventCreated)
}

// SubscribeDeleted subscribes to queue.queueDeleted events on the system bus.
// The handler receives a DeletedPayload.
func SubscribeDeleted(handler func(DeletedPayload)) (*eventbus.Subscription, error) {
	return eventbus.System().Subscribe(func(_ string, args []interface{}) {
		if len(args) < 1 {
			return
		}
		var queue publicqueue.Params
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		handler(DeletedPayload{Queue: queue})
	}, EventDeleted)
}

// SubscribeStateChanged subscribes to queue.stateChanged events on the system bus.
// The handler receives a StateChangedPayload.
func SubscribeStateChanged(handler func(StateChangedPayload)) (*eventbus.Subscription, error) {
	return eventbus.System().Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}
		var queue publicqueue.Params
		var transition publicqueue.StateTransition
		if err := decodeArg(args[0], &queue); err != nil {
			return
		}
		if err := decodeArg(args[1], &transition); err != nil {
			return
		}
		handler(StateChangedPayload{Queue: queue, Transition: transition})
	}, EventStateChanged)
}

// SubscribeConsumerGroupCreated subscribes to queue.consumerGroupCreated events
// on the system bus.
func SubscribeConsumerGroupCreated(handler func(ConsumerGroupCreatedPayload)) (*eventbus.Subscription, error) {
	return eventbus.System().Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}
		var queue publicqueue.Params
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

// SubscribeConsumerGroupDeleted subscribes to queue.consumerGroupDeleted events
// on the system bus.
func SubscribeConsumerGroupDeleted(handler func(ConsumerGroupDeletedPayload)) (*eventbus.Subscription, error) {
	return eventbus.System().Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}
		var queue publicqueue.Params
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

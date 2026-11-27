/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package scheduler

import (
	"encoding/json"
	"fmt"

	"github.com/weyoss/go-redis-smq/pkg/eventbus"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// MessagePublishedPayload is the payload for scheduler.messagePublished
// events. Fires when a scheduled message (simple, CRON-triggered, or
// repeat-clone) is moved into a pending list.
//
// For repeating messages, MessageID is the clone's ID — the parent stays
// in the scheduled set for its next tick.
type MessagePublishedPayload struct {
	MessageID string
	Queue     queue.ParsedParams
}

// MessageDeadLetteredPayload is the payload for
// scheduler.messageDeadLettered events. Fires when a scheduled message
// could not be delivered because its target consumer group no longer
// existed at fire time.
//
// For repeating messages, MessageID is the parent's ID — the cycle is
// terminated by dead-lettering the parent; the would-be clone for the
// current firing is discarded.
type MessageDeadLetteredPayload struct {
	MessageID string
	Queue     queue.ParsedParams
}

func decodeArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

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

// SubscribeMessagePublished registers a handler for the
// scheduler.messagePublished event.
func SubscribeMessagePublished(handler func(MessagePublishedPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("scheduler events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}

		var messageID string
		var queueParsed queue.ParsedParams

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeQueueArg(args[1], &queueParsed); err != nil {
			return
		}

		handler(MessagePublishedPayload{
			MessageID: messageID,
			Queue:     queueParsed,
		})
	}, EventMessagePublished)
}

// SubscribeMessageDeadLettered registers a handler for the
// scheduler.messageDeadLettered event.
func SubscribeMessageDeadLettered(handler func(MessageDeadLetteredPayload)) (eventbus.Subscription, error) {
	bus := eventbus.UserBus()
	if bus == nil {
		return nil, fmt.Errorf("scheduler events: user event bus not configured")
	}

	return bus.Subscribe(func(_ string, args []interface{}) {
		if len(args) < 2 {
			return
		}

		var messageID string
		var queueParsed queue.ParsedParams

		if err := decodeArg(args[0], &messageID); err != nil {
			return
		}
		if err := decodeQueueArg(args[1], &queueParsed); err != nil {
			return
		}

		handler(MessageDeadLetteredPayload{
			MessageID: messageID,
			Queue:     queueParsed,
		})
	}, EventMessageDeadLettered)
}

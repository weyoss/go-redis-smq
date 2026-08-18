/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package events defines internal queue event names and payload types.
//
// These events are published through the event multiplexer to the system
// bus, user bus, or both, depending on the event routing policy.
package events

import (
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Queue event names.
const (
	EventCreated              = "queue.queueCreated"
	EventDeleted              = "queue.queueDeleted"
	EventStateChanged         = "queue.stateChanged"
	EventConsumerGroupCreated = "queue.consumerGroupCreated"
	EventConsumerGroupDeleted = "queue.consumerGroupDeleted"
)

// CreatedPayload is used by public subscribers to receive the arguments
// of a queue.queueCreated event.
type CreatedPayload struct {
	Queue      q.QueueParams `json:"queue"`
	Properties q.QueueProps  `json:"properties"`
}

// DeletedPayload is used by public subscribers to receive the arguments
// of a queue.queueDeleted event.
type DeletedPayload struct {
	Queue q.QueueParams `json:"queue"`
}

// StateChangedPayload is used by public subscribers to receive the
// arguments of a queue.stateChanged event.
type StateChangedPayload struct {
	Queue      q.QueueParams     `json:"queue"`
	Transition q.StateTransition `json:"transition"`
}

// ConsumerGroupCreatedPayload is used by public subscribers to receive
// the arguments of a queue.consumerGroupCreated event.
type ConsumerGroupCreatedPayload struct {
	Queue   q.QueueParams `json:"queue"`
	GroupID string        `json:"groupId"`
}

// ConsumerGroupDeletedPayload is used by public subscribers to receive
// the arguments of a queue.consumerGroupDeleted event.
type ConsumerGroupDeletedPayload struct {
	Queue   q.QueueParams `json:"queue"`
	GroupID string        `json:"groupId"`
}

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
// These events are published through the event multiplexer to the
// appropriate bus (system or user) according to the event routing policy.
// The payload structs here are used by internal subscription functions and
// internal publishers; they are separate from the public payload types in
// pkg/queue to avoid leaking internal details.
package events

import (
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Queue event names.
const (
	EventCreated              = "queue.queueCreated"
	EventDeleted              = "queue.queueDeleted"
	EventStateChanged         = "queue.stateChanged"
	EventConsumerGroupCreated = "queue.consumerGroupCreated"
	EventConsumerGroupDeleted = "queue.consumerGroupDeleted"
)

// CreatedPayload is used by internal subscribers to receive the arguments
// of a queue.queueCreated event.
type CreatedPayload struct {
	Queue      publicqueue.Params
	Properties publicqueue.Props
}

// DeletedPayload is used by internal subscribers to receive the arguments
// of a queue.queueDeleted event.
type DeletedPayload struct {
	Queue publicqueue.Params
}

// StateChangedPayload is used by internal subscribers to receive the
// arguments of a queue.stateChanged event.
type StateChangedPayload struct {
	Queue      publicqueue.Params
	Transition publicqueue.StateTransition
}

// ConsumerGroupCreatedPayload is used by internal subscribers to receive
// the arguments of a queue.consumerGroupCreated event.
type ConsumerGroupCreatedPayload struct {
	Queue   publicqueue.Params
	GroupID string
}

// ConsumerGroupDeletedPayload is used by internal subscribers to receive
// the arguments of a queue.consumerGroupDeleted event.
type ConsumerGroupDeletedPayload struct {
	Queue   publicqueue.Params
	GroupID string
}

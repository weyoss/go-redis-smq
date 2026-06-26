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
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

const (
	EventCreated              = "queue.queueCreated"
	EventDeleted              = "queue.queueDeleted"
	EventStateChanged         = "queue.stateChanged"
	EventConsumerGroupCreated = "queue.consumerGroupCreated"
	EventConsumerGroupDeleted = "queue.consumerGroupDeleted"
)

type CreatedPayload struct {
	Queue      q.QueueParams `json:"queue"`
	Properties q.QueueProps  `json:"properties"`
}

type DeletedPayload struct {
	Queue q.QueueParams `json:"queue"`
}

type StateChangedPayload struct {
	Queue      q.QueueParams     `json:"queue"`
	Transition q.StateTransition `json:"transition"`
}

type ConsumerGroupCreatedPayload struct {
	Queue   q.QueueParams `json:"queue"`
	GroupID string        `json:"groupId"`
}

type ConsumerGroupDeletedPayload struct {
	Queue   q.QueueParams `json:"queue"`
	GroupID string        `json:"groupId"`
}

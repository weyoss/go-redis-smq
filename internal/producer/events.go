/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer

import (
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Producer event names.
const (
	EventUp               = "producer.up"
	EventDown             = "producer.down"
	EventGoingUp          = "producer.goingUp"
	EventGoingDown        = "producer.goingDown"
	EventMessagePublished = "producer.messagePublished"
)

// ── Lifecycle payload ──

// LifecyclePayload is used by public subscribers to receive the producer ID
// from producer lifecycle events.
type LifecyclePayload struct {
	ProducerID string `json:"producerId"`
}

// ── Message payload ──

// MessagePublishedPayload is used by public subscribers to receive the
// arguments of a producer.messagePublished event.
//
// It matches the TypeScript event signature:
//
//	(messageId: string, queue: IQueueParsedParams, producerId: string) => void
type MessagePublishedPayload struct {
	MessageID  string       `json:"messageId"`
	Queue      queue.Params `json:"queue"`
	ProducerID string       `json:"producerId"`
}

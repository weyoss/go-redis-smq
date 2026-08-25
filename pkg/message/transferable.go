/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package message

import (
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Transferable holds the complete message as it is transferred between
// producers, Redis, and consumers.
//
// It matches the TypeScript IMessageTransferable format and is safe for
// JSON serialization across RedisSMQ implementations.
type Transferable struct {
	// ID is the unique message identifier.
	ID string `json:"id"`

	// CreatedAt is the Unix timestamp in milliseconds when the message was
	// created.
	CreatedAt int64 `json:"createdAt"`

	// TTL is the message time-to-live in milliseconds. A value of 0 means
	// the message never expires.
	TTL int64 `json:"ttl"`

	// RetryThreshold is the maximum number of retry attempts before the
	// message is dead-lettered.
	RetryThreshold int `json:"retryThreshold"`

	// RetryDelay is the delay between retry attempts in milliseconds.
	RetryDelay int64 `json:"retryDelay"`

	// ConsumeTimeout is the maximum time a consumer has to process the
	// message, in milliseconds. A value of 0 means no timeout.
	ConsumeTimeout int64 `json:"consumeTimeout"`

	// Body is the message payload. It can be any JSON-serializable value.
	Body interface{} `json:"body"`

	// Priority is the message priority, if set. It is only relevant for
	// priority queues.
	Priority *MessagePriority `json:"priority,omitempty"`

	// ScheduledCron is the CRON expression used for scheduled delivery.
	// Empty when not set.
	ScheduledCron string `json:"scheduledCron,omitempty"`

	// ScheduledDelay is the initial delivery delay in milliseconds, if set.
	ScheduledDelay *int64 `json:"scheduledDelay,omitempty"`

	// ScheduledRepeatPeriod is the repeat period in milliseconds, if set.
	ScheduledRepeatPeriod *int64 `json:"scheduledRepeatPeriod,omitempty"`

	// ScheduledRepeat is the number of repeat deliveries. A value of 0
	// means no repeats.
	ScheduledRepeat int `json:"scheduledRepeat"`

	// Exchange is the exchange used for routing, if the message was sent
	// via an exchange.
	Exchange *x.ExchangeParams `json:"exchange,omitempty"`

	// Queue is the target queue, if the message was sent directly to a
	// queue.
	Queue *queue.QueueParams `json:"queue,omitempty"`

	// DestinationQueue is the resolved destination queue for the message.
	DestinationQueue *queue.QueueParams `json:"destinationQueue"`

	// ConsumerGroupID is the consumer group the message belongs to for
	// Pub/Sub queues.
	ConsumerGroupID string `json:"consumerGroupId,omitempty"`

	// MessageState contains lifecycle state information such as attempts,
	// timestamps, and counters.
	MessageState StateTransferable `json:"messageState"`

	// Status is the current message status.
	Status MessageStatus `json:"status"`
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package msg

import (
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Transferable holds the complete message for transfer between systems.
type Transferable struct {
	ID                    string            `json:"id"`
	CreatedAt             int64             `json:"createdAt"`
	TTL                   int64             `json:"ttl"`
	RetryThreshold        int               `json:"retryThreshold"`
	RetryDelay            int64             `json:"retryDelay"`
	ConsumeTimeout        int64             `json:"consumeTimeout"`
	Body                  interface{}       `json:"body"`
	Priority              *MessagePriority  `json:"priority,omitempty"`
	ScheduledCron         string            `json:"scheduledCron,omitempty"`
	ScheduledDelay        *int64            `json:"scheduledDelay,omitempty"`
	ScheduledRepeatPeriod *int64            `json:"scheduledRepeatPeriod,omitempty"`
	ScheduledRepeat       int               `json:"scheduledRepeat"`
	Exchange              *x.ExchangeParams `json:"exchange,omitempty"`
	Queue                 *q.QueueParams    `json:"queue,omitempty"`
	DestinationQueue      *q.QueueParams    `json:"destinationQueue"`
	ConsumerGroupID       string            `json:"consumerGroupId,omitempty"`
	MessageState          StateTransferable `json:"messageState"`
	Status                MessageStatus     `json:"status"`
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package q

import "time"

// QueueProps holds the stored configuration of a queue.
type QueueProps struct {
	Type                      QueueType
	DeliveryModel             DeliveryModel
	OperationalState          QueueState
	MessagesCount             int64
	ScheduledMessagesCount    int64
	PendingMessagesCount      int64
	ProcessingMessagesCount   int64
	AcknowledgedMessagesCount int64
	DeadLetteredMessagesCount int64
	DelayedMessagesCount      int64
	RequeuedMessagesCount     int64
	RateLimit                 *RateLimitParams
	CreatedAt                 time.Time
	LastStateChangeAt         time.Time
	LockID                    string
}

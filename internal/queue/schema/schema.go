/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package schema

import "strconv"

// QueueField identifies queue property fields in Redis hash storage.
type QueueField int

const (
	QueueFieldType                      QueueField = iota // 0
	QueueFieldRateLimit                                   // 1
	QueueFieldMessagesCount                               // 2
	QueueFieldDeliveryModel                               // 3
	QueueFieldScheduledMessagesCount                      // 4
	QueueFieldPendingMessagesCount                        // 5
	QueueFieldProcessingMessagesCount                     // 6
	QueueFieldAcknowledgedMessagesCount                   // 7
	QueueFieldDeadLetteredMessagesCount                   // 8
	QueueFieldDelayedMessagesCount                        // 9
	QueueFieldRequeuedMessagesCount                       // 10
	QueueFieldOperationalState                            // 11
	QueueFieldLastStateChangeAt                           // 12
	QueueFieldLockID                                      // 13
)

func (f QueueField) Key() string { return strconv.Itoa(int(f)) }
func (f QueueField) Int() int    { return int(f) }

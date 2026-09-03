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

// Field identifies queue property fields in Redis hash storage.
type Field int

const (
	Type                      Field = iota // 0
	RateLimit                              // 1
	MessagesCount                          // 2
	DeliveryModel                          // 3
	ScheduledMessagesCount                 // 4
	PendingMessagesCount                   // 5
	ProcessingMessagesCount                // 6
	AcknowledgedMessagesCount              // 7
	DeadLetteredMessagesCount              // 8
	DelayedMessagesCount                   // 9
	RequeuedMessagesCount                  // 10
	OperationalState                       // 11
	LastStateChangeAt                      // 12
	LockID                                 // 13
)

func (f Field) Key() string { return strconv.Itoa(int(f)) }
func (f Field) Int() int    { return int(f) }

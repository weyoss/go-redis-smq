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

// MessageStatus represents the current lifecycle state of a message.
// Integer values are persisted in Redis and must not be changed.
type MessageStatus int

const (
	StatusNew            MessageStatus = iota // 0
	StatusPending                             // 1
	StatusProcessing                          // 2
	StatusScheduled                           // 3
	StatusAcknowledged                        // 4
	StatusUnackRequeuing                      // 5
	StatusUnackDelaying                       // 6
	StatusDeadLettered                        // 7
)

func (s MessageStatus) Int() int { return int(s) }

func (s MessageStatus) String() string {
	switch s {
	case StatusNew:
		return "new"
	case StatusPending:
		return "pending"
	case StatusProcessing:
		return "processing"
	case StatusScheduled:
		return "scheduled"
	case StatusAcknowledged:
		return "acknowledged"
	case StatusUnackRequeuing:
		return "unack_requeuing"
	case StatusUnackDelaying:
		return "unack_delaying"
	case StatusDeadLettered:
		return "dead_lettered"
	default:
		return "unknown"
	}
}

func (s MessageStatus) IsTerminal() bool   { return s == StatusAcknowledged || s == StatusDeadLettered }
func (s MessageStatus) IsPending() bool    { return s == StatusPending }
func (s MessageStatus) IsProcessing() bool { return s == StatusProcessing }
func (s MessageStatus) IsRequeuable() bool { return s == StatusAcknowledged || s == StatusDeadLettered }
func (s MessageStatus) IsValid() bool      { return s >= StatusNew && s <= StatusDeadLettered }

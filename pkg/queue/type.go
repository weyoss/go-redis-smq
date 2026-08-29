/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue

// Type represents the ordering semantics of a queue.
// Values match TypeScript EQueueType enum for cross-language compatibility.
// Integer values are persisted in Redis and must not be changed.
type Type int

const (
	// TypeLIFO delivers messages in last-in-first-out order.
	TypeLIFO Type = iota // 0

	// TypeFIFO delivers messages in first-in-first-out order.
	TypeFIFO // 1

	// TypePriority delivers messages based on priority level.
	TypePriority // 2
)

// Int returns the integer representation for Redis storage.
func (t Type) Int() int { return int(t) }

// String returns a human-readable representation.
// Unknown values return "unknown".
func (t Type) String() string {
	switch t {
	case TypeFIFO:
		return "fifo"
	case TypeLIFO:
		return "lifo"
	case TypePriority:
		return "priority"
	default:
		return "unknown"
	}
}

// IsValid reports whether the queue type value is within the valid range.
func (t Type) IsValid() bool {
	return t >= TypeLIFO && t <= TypePriority
}

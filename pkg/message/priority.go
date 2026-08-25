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

// MessagePriority represents the priority level of a message.
//
// Lower integer values indicate higher priority. The values are persisted
// in Redis and must match the TypeScript EMessagePriority enum for
// cross-language compatibility.
type MessagePriority int

const (
	// PriorityHighest is the highest priority level (0).
	PriorityHighest MessagePriority = iota

	// PriorityVeryHigh is the second highest priority level (1).
	PriorityVeryHigh

	// PriorityHigh is the third highest priority level (2).
	PriorityHigh

	// PriorityAboveNormal is above the normal priority level (3).
	PriorityAboveNormal

	// PriorityNormal is the standard priority level (4).
	PriorityNormal

	// PriorityLow is below the normal priority level (5).
	PriorityLow

	// PriorityVeryLow is the second lowest priority level (6).
	PriorityVeryLow

	// PriorityLowest is the lowest priority level (7).
	PriorityLowest
)

// Int returns the integer representation for Redis storage.
func (p MessagePriority) Int() int { return int(p) }

// String returns a human-readable representation.
// Unknown values return "unknown".
func (p MessagePriority) String() string {
	switch p {
	case PriorityHighest:
		return "highest"
	case PriorityVeryHigh:
		return "very_high"
	case PriorityHigh:
		return "high"
	case PriorityAboveNormal:
		return "above_normal"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	case PriorityVeryLow:
		return "very_low"
	case PriorityLowest:
		return "lowest"
	default:
		return "unknown"
	}
}

// IsValid reports whether the priority value is within the valid range.
func (p MessagePriority) IsValid() bool {
	return p >= PriorityHighest && p <= PriorityLowest
}

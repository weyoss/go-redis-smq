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

// MessagePriority represents message priority levels.
// Lower values indicate higher priority.
// Integer values are persisted in Redis and must not be changed.
type MessagePriority int

const (
	PriorityHighest     MessagePriority = iota // 0
	PriorityVeryHigh                           // 1
	PriorityHigh                               // 2
	PriorityAboveNormal                        // 3
	PriorityNormal                             // 4
	PriorityLow                                // 5
	PriorityVeryLow                            // 6
	PriorityLowest                             // 7
)

func (p MessagePriority) Int() int { return int(p) }

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

func (p MessagePriority) IsValid() bool { return p >= PriorityHighest && p <= PriorityLowest }

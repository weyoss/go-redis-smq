/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package exchange

// Type defines how an exchange routes messages to queues.
// Integer values are persisted in Redis and must not be changed.
type Type int

const (
	// TypeDirect routes messages to queues with an exact matching routing key.
	TypeDirect Type = 0

	// TypeFanout broadcasts messages to all bound queues, ignoring routing keys.
	TypeFanout Type = 1

	// TypeTopic routes messages using AMQP-style pattern matching (*, #).
	TypeTopic Type = 2
)

// Int returns the integer representation for Redis storage.
func (t Type) Int() int { return int(t) }

// String returns a human-readable representation.
func (t Type) String() string {
	switch t {
	case TypeDirect:
		return "direct"
	case TypeFanout:
		return "fanout"
	case TypeTopic:
		return "topic"
	default:
		return "unknown"
	}
}

// IsValid reports whether the type value is within the valid range.
func (t Type) IsValid() bool { return t >= TypeDirect && t <= TypeTopic }

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

// ExchangeType defines how an exchange routes messages to queues.
// Integer values are persisted in Redis and must not be changed.
type ExchangeType int

const (
	// TypeDirect routes messages to queues with an exact matching routing key.
	TypeDirect ExchangeType = 0

	// TypeFanout broadcasts messages to all bound queues, ignoring routing keys.
	TypeFanout ExchangeType = 1

	// TypeTopic routes messages using AMQP-style pattern matching (*, #).
	TypeTopic ExchangeType = 2
)

// Int returns the integer representation for Redis storage.
func (t ExchangeType) Int() int { return int(t) }

// String returns a human-readable representation.
func (t ExchangeType) String() string {
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
func (t ExchangeType) IsValid() bool { return t >= TypeDirect && t <= TypeTopic }

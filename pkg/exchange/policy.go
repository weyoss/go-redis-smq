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

// Policy restricts which queue types can bind to an exchange.
// Integer values are persisted in Redis and must not be changed.
type Policy int

const (
	// PolicyStandard allows only FIFO and LIFO queues.
	// Standard exchanges require ordered message delivery.
	PolicyStandard Policy = iota // 0

	// PolicyPriority allows only Priority queues.
	// Priority exchanges require message prioritization.
	PolicyPriority // 1
)

// Int returns the integer representation for Redis storage.
func (p Policy) Int() int { return int(p) }

// String returns a human-readable representation.
func (p Policy) String() string {
	switch p {
	case PolicyStandard:
		return "standard"
	case PolicyPriority:
		return "priority"
	default:
		return "unknown"
	}
}

// IsValid reports whether the policy value is within the valid range.
func (p Policy) IsValid() bool { return p >= PolicyStandard && p <= PolicyPriority }

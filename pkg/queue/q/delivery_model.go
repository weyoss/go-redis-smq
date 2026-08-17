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

// DeliveryModel defines how messages are delivered to consumers.
// Integer values are persisted in Redis and must not be changed.
type DeliveryModel int

const (
	// DeliveryPointToPoint delivers each message to exactly one consumer.
	// Best for task queues and workload distribution.
	DeliveryPointToPoint DeliveryModel = iota // 0

	// DeliveryPubSub delivers each message to all consumers in a group.
	// Best for event broadcasting and fan-out patterns.
	DeliveryPubSub // 1
)

// Int returns the integer representation for Redis storage.
func (dm DeliveryModel) Int() int { return int(dm) }

// String returns a human-readable representation.
// Unknown values return "unknown".
func (dm DeliveryModel) String() string {
	switch dm {
	case DeliveryPointToPoint:
		return "point_to_point"
	case DeliveryPubSub:
		return "pub_sub"
	default:
		return "unknown"
	}
}

// IsValid reports whether the delivery model value is within the valid range.
func (dm DeliveryModel) IsValid() bool {
	return dm >= DeliveryPointToPoint && dm <= DeliveryPubSub
}

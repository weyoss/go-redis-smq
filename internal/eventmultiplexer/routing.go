/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package eventmultiplexer

// routingPolicies maps each RedisSMQ event to its destination bus target.
//
// The default policy for events not listed here is TargetUser, meaning
// producer and consumer events are public-only. Queue lifecycle events are
// sent to both buses because they are needed internally for cache
// synchronisation and are also useful for external monitoring.
// Configuration updates are internal only.
var routingPolicies = map[string]Target{
	"queue.queueCreated":         TargetBoth,
	"queue.queueDeleted":         TargetBoth,
	"queue.consumerGroupCreated": TargetBoth,
	"queue.consumerGroupDeleted": TargetBoth,
	"queue.stateChanged":         TargetBoth,
	"configuration.updated":      TargetSystem,
}

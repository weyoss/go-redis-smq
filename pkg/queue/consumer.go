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

// QueueConsumer describes a single consumer registered for a queue.
//
// The wire format matches TypeScript's IQueueConsumer interface:
//
//	{
//	  "ipAddress": ["10.0.0.1"],
//	  "hostname": "worker-1",
//	  "pid": 12345,
//	  "createdAt": 1763942400000
//	}
//
// Values are captured once, at subscription time, and stored in the
// queue's Consumers hash. They are not refreshed while the consumer is
// subscribed — a hostname or IP change is not reflected until the
// consumer re-registers (unsubscribe + subscribe).
//
// The field name IPAddresses is Go-idiomatic (plural for a slice). The
// JSON wire name is "ipAddress" (singular) — matching the TypeScript
// interface, whose ipAddress field is typed as string[].
//
// nolint:revive // "Consumer" would collide conceptually with
// consumer.Consumer when both packages are imported.
type QueueConsumer struct {
	// IPAddresses holds every non-internal IPv4 address of the host as
	// reported at subscription time. A host with multiple interfaces
	// (or multiple addresses on one interface) contributes multiple
	// entries. Empty if the host reports no non-internal IPv4 addresses.
	IPAddresses []string `json:"ipAddress"`

	// Hostname is the host's name as reported at subscription time.
	Hostname string `json:"hostname"`

	// PID is the process ID of the consumer.
	PID int `json:"pid"`

	// CreatedAt is the Unix-millisecond timestamp at which the consumer
	// registered. Used by the reaper to identify stale consumers whose
	// heartbeat has expired.
	CreatedAt int64 `json:"createdAt"`
}

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

// LockOwner identifies the entity holding a queue lock.
//
// The integer value is persisted in Redis and must match the TypeScript
// EQueueStateLockOwner enum for cross-language compatibility.
type LockOwner int

const (
	// LockOwnerPurgeJob indicates a queue purge operation holds the lock.
	LockOwnerPurgeJob LockOwner = iota // 0
)

// String returns a human-readable representation.
// Unknown values return "UNKNOWN".
func (o LockOwner) String() string {
	switch o {
	case LockOwnerPurgeJob:
		return "PURGE_JOB"
	default:
		return "UNKNOWN"
	}
}

// Int returns the integer representation for Redis storage.
func (o LockOwner) Int() int { return int(o) }

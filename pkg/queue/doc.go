/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package queue provides a high-level, user-facing API for managing
// RedisSMQ queues.
//
// It includes operations for creating, inspecting, listing, pausing,
// resuming, stopping, and deleting queues. The package also provides
// rate‑limiting controls, state management, consumer groups, message
// browsing, and purge operations.
//
// Most functions are available both as methods on a Manager instance
// (created with NewManager) and as package‑level convenience functions
// that use a default manager.
package queue

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package consumer provides the public API for creating and managing
// RedisSMQ consumers.
//
// A consumer subscribes to one or more queues and processes messages using
// user-defined handlers. It manages heartbeats, background workers, and
// graceful shutdown automatically.
package consumer

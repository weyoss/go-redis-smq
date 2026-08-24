/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package eventbus defines public interfaces for the RedisSMQ event bus.
//
// These interfaces are used by public subscription functions in domain
// packages (such as consumer, queue, and producer) to allow external
// applications to observe system events. The concrete implementation is
// provided internally and wired by the root redissmq package.
package eventbus

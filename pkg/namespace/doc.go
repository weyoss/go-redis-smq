/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package namespace provides namespace-level operations for RedisSMQ.
//
// A namespace is a logical isolation boundary that contains queues and
// exchanges. Namespace names are validated using Redis key rules (lowercase,
// letter-first, alphanumeric with hyphens, underscores, and dots).
//
// The concrete implementation is provided by the root redissmq package and
// is created via redissmq.NewNamespaceManager().
package namespace

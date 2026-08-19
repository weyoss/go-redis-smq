/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package exchange provides the public API for managing RedisSMQ exchanges.
//
// Exchanges route messages from producers to one or more queues. This package
// includes managers for common operations and type-specific helpers for
// direct, topic, and fanout exchanges.
//
// Example:
//
//	params := x.MustExchangeParams("orders", x.TypeDirect)
//	err := exchange.Create(ctx, params, x.PolicyStandard)
package exchange

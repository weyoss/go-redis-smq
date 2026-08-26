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
// # Concrete Implementation
//
// The package defines the Manager interface and the sentinel errors that
// namespace operations may return. The concrete implementation is provided by
// the root redissmq package and is created using
// redissmq.NewNamespaceManager().
//
// # Example
//
//	nm := redissmq.NewNamespaceManager()
//	namespaces, err := nm.List(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, ns := range namespaces {
//	    fmt.Println(ns)
//	}
package namespace

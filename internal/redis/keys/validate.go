/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package keys provides Redis key builders and validation for RedisSMQ.
//
// Key validation is delegated to the public pkg/redis package to keep a
// single source of truth and allow public packages to validate keys
// without importing internal code.
package keys

import (
	"github.com/weyoss/go-redis-smq/pkg/redis"
)

// ErrInvalidKey is an alias for the public redis key validation error.
var ErrInvalidKey = redis.ErrInvalidKey

// ValidateKey validates a Redis key using the public pkg/redis validator.
// It returns the lowercased valid key or an error.
func ValidateKey(key string) (string, error) {
	return redis.ValidateKey(key)
}

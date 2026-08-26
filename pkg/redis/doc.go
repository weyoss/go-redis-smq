/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package redis provides small public utilities related to Redis key
// validation used by RedisSMQ public packages.
//
// The package is intentionally minimal. It defines the error and validation
// function needed to enforce Redis key naming rules for queues, exchanges,
// and namespaces across the public API.
//
// # Key Validation
//
// ValidateKey ensures a key follows the required format: lowercase letters,
// digits, hyphens, underscores, and dots. The first character must be a
// letter. The input is lowercased automatically before validation, and the
// validated key is returned. It returns ErrInvalidKey if validation fails.
//
// # Example
//
//	key, err := redis.ValidateKey("My-Queue.Name")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(key) // "my-queue.name"
package redis

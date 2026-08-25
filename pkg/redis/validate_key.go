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
package redis

import (
	"errors"
	"strings"
)

// ErrInvalidKey indicates that a Redis key is invalid.
var ErrInvalidKey = errors.New("invalid redis key")

// ValidateKey ensures a Redis key follows the required format:
// lowercase letters, digits, hyphens, underscores, and dots. The first
// character must be a letter.
func ValidateKey(key string) (string, error) {
	if key == "" {
		return "", ErrInvalidKey
	}

	key = strings.ToLower(key)

	// First character must be a letter
	if !isLetter(key[0]) {
		return "", ErrInvalidKey
	}

	// Remaining characters can be letters, numbers, or special chars
	for i := 1; i < len(key); i++ {
		if !isValidKeyChar(key[i]) {
			return "", ErrInvalidKey
		}
	}

	return key, nil
}

func isLetter(c byte) bool {
	return c >= 'a' && c <= 'z'
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isValidKeyChar(c byte) bool {
	return isLetter(c) || isDigit(c) || c == '-' || c == '_' || c == '.'
}

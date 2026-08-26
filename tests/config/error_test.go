/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package config_test

import (
	"testing"

	"github.com/weyoss/go-redis-smq/pkg/config"
)

// Note: config.Get() panics if called before Init.
// Error scenarios for config are limited — Init/Get/Save are the only public operations.
// Version mismatch and other save errors are covered in save_test.go.

// Scenario: Error variables are defined
func TestError_ErrorVariablesExist(t *testing.T) {
	// Compile-time check that error sentinels exist.
	// These are tested indirectly through Save operations in save_test.go.
	_ = config.ErrVersionMismatch
	_ = config.ErrNotInitialized
	_ = config.ErrInvalidConfig
}

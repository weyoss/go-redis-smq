/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package errs provides shared sentinel errors for the internal layer.
// Public packages define their own errors
package errs

import "errors"

var (
	// ErrUnexpectedScriptReply indicates a Lua script returned an unexpected value type.
	ErrUnexpectedScriptReply = errors.New("unexpected script reply")

	// ErrInvalidArgs indicates invalid arguments were passed to a function.
	ErrInvalidArgs = errors.New("invalid arguments")

	// ErrNilValue indicates a nil value was passed where a non-nil value is required.
	ErrNilValue = errors.New("value is nil")

	// ErrEmptyValue indicates an empty value was passed where a non-empty value is required.
	ErrEmptyValue = errors.New("value is empty")
)

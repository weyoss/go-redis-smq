/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package codec

import (
	"errors"
	"fmt"
)

// Sentinel errors for codec operations.
var (
	// ErrEncoding indicates a failure during the encoding process.
	ErrEncoding = errors.New("encode failed")

	// ErrDecoding indicates a failure during the decoding process.
	ErrDecoding = errors.New("decode failed")

	// ErrInvalidFormat indicates the data format is not valid for decoding.
	ErrInvalidFormat = errors.New("invalid format")

	// ErrUnsupportedType indicates the type cannot be encoded or decoded.
	ErrUnsupportedType = errors.New("unsupported type")
)

// EncodingError wraps encoding errors with type and entity context.
// Implements the error unwrapping interface for errors.Is/As support.
type EncodingError struct {
	Type   string // The Go type being encoded (e.g., "exchange params")
	Entity string // The specific entity identifier (e.g., exchange name)
	Err    error  // The underlying error
}

// NewEncodingError creates a new EncodingError with context.
func NewEncodingError(typ, entity string, err error) *EncodingError {
	return &EncodingError{
		Type:   typ,
		Entity: entity,
		Err:    err,
	}
}

// Error returns a formatted error message with type and entity context.
func (e *EncodingError) Error() string {
	if e.Entity != "" {
		return fmt.Sprintf("encode %s %s: %v", e.Type, e.Entity, e.Err)
	}
	return fmt.Sprintf("encode %s: %v", e.Type, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *EncodingError) Unwrap() error { return e.Err }

// DecodingError wraps decoding errors with type and entity context.
// Implements the error unwrapping interface for errors.Is/As support.
type DecodingError struct {
	Type   string // The Go type being decoded (e.g., "exchange props")
	Entity string // The specific entity identifier (e.g., hash data)
	Err    error  // The underlying error
}

// NewDecodingError creates a new DecodingError with context.
func NewDecodingError(typ, entity string, err error) *DecodingError {
	return &DecodingError{
		Type:   typ,
		Entity: entity,
		Err:    err,
	}
}

// Error returns a formatted error message with type and entity context.
func (e *DecodingError) Error() string {
	if e.Entity != "" {
		return fmt.Sprintf("decode %s %s: %v", e.Type, e.Entity, e.Err)
	}
	return fmt.Sprintf("decode %s: %v", e.Type, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *DecodingError) Unwrap() error { return e.Err }

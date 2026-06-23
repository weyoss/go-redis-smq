/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package codec provides interfaces and implementations for serializing
// domain types to and from Redis storage formats.
//
// The codec package is internal to avoid exposing serialization details
// in the public API. It ensures compatible data formats
// for cross-language interoperability.
//
// Codecs handle two Redis storage formats:
//   - Hash: map[string]string for entity properties (HSET/HGETALL)
//   - Set: string members for indexes and registries (SADD/SMEMBERS)
package codec

import "context"

// SetEncoder serializes a Go value to a string for Redis set storage.
// Redis sets store string members, so complex types must be encoded
// as strings (typically JSON).
type SetEncoder[T any] interface {
	// EncodeSet converts a value to its Redis set string representation.
	EncodeSet(ctx context.Context, value T) (string, error)
}

// SetDecoder deserializes a string from a Redis set back to a Go value.
type SetDecoder[T any] interface {
	// DecodeSet reconstructs a value from its Redis set string representation.
	DecodeSet(ctx context.Context, data string) (T, error)
}

// SetCodec combines encoding and decoding for Redis set storage.
// Used for types stored as JSON strings in Redis sets (e.g., indexes, registries).
type SetCodec[T any] interface {
	SetEncoder[T]
	SetDecoder[T]
}

// HashEncoder serializes a Go value to a Redis hash map.
// Redis hashes store string field-value pairs, so all values
// must be convertible to strings.
type HashEncoder[T any] interface {
	// EncodeHash converts a value to a map suitable for Redis HSET.
	// Keys are field names, values are string-compatible types.
	EncodeHash(ctx context.Context, value T) (map[string]interface{}, error)
}

// HashDecoder deserializes a Redis hash map back to a Go value.
// Redis HGETALL returns map[string]string, so the decoder must
// handle string-to-type conversions.
type HashDecoder[T any] interface {
	// DecodeHash reconstructs a value from a Redis hash map.
	DecodeHash(ctx context.Context, hash map[string]string) (T, error)
}

// HashCodec combines encoding and decoding for Redis hash storage.
// Used for types stored as Redis hashes with integer field keys
type HashCodec[T any] interface {
	HashEncoder[T]
	HashDecoder[T]
}

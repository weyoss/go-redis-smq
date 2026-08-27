/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue

import (
	"encoding/json"
	"fmt"
	"time"
)

// RateLimitParams represents rate limiting configuration for a queue.
//
// JSON format matches TypeScript IRateLimitParams for cross-language compatibility:
//
//	{
//	  "limit": 100,
//	  "interval": 60000
//	}
//
// Note: "interval" is serialized as milliseconds to match TypeScript.
type RateLimitParams struct {
	limit    int
	interval time.Duration
}

// NewRateLimitParams creates validated rate limit parameters.
//
//	limit    - maximum number of messages allowed in the interval (must be > 0)
//	interval - time window for the limit (must be >= 1 second)
//
// Example:
//
//	rl, err := queue.NewRateLimitParams(100, time.Minute)
func NewRateLimitParams(limit int, interval time.Duration) (*RateLimitParams, error) {
	p := &RateLimitParams{limit: limit, interval: interval}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

// Validate checks if rate limit parameters are valid.
// The limit must be greater than zero and the interval must be at least one second.
func (p *RateLimitParams) Validate() error {
	if p == nil {
		return ErrInvalidRateLimit
	}
	if p.limit <= 0 {
		return ErrInvalidRateLimit
	}
	if p.interval < time.Second {
		return ErrInvalidRateLimitInterval
	}
	return nil
}

// Limit returns the maximum number of messages allowed.
// It returns 0 if the receiver is nil.
func (p *RateLimitParams) Limit() int {
	if p == nil {
		return 0
	}
	return p.limit
}

// Interval returns the time window duration.
// It returns 0 if the receiver is nil.
func (p *RateLimitParams) Interval() time.Duration {
	if p == nil {
		return 0
	}
	return p.interval
}

// MarshalJSON implements custom JSON marshaling for cross-language compatibility.
// It uses a value receiver so that both RateLimitParams values and pointers
// implement json.Marshaler.
func (p RateLimitParams) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Limit    int   `json:"limit"`
		Interval int64 `json:"interval"`
	}{
		Limit:    p.limit,
		Interval: p.interval.Milliseconds(),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling from TypeScript format.
// It expects interval in milliseconds.
func (p *RateLimitParams) UnmarshalJSON(data []byte) error {
	var aux struct {
		Limit    int   `json:"limit"`
		Interval int64 `json:"interval"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	p.limit = aux.Limit
	p.interval = time.Duration(aux.Interval) * time.Millisecond
	return p.Validate()
}

// String returns a human-readable representation.
// It returns an empty string if the receiver is nil.
func (p *RateLimitParams) String() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%d messages per %s", p.limit, p.interval)
}

// MustRateLimitParams creates rate limit params and panics on error.
// Useful for initialization where params are known to be valid.
//
// Example:
//
//	rl := queue.MustRateLimitParams(100, time.Minute)
func MustRateLimitParams(limit int, interval time.Duration) *RateLimitParams {
	p, err := NewRateLimitParams(limit, interval)
	if err != nil {
		panic(err)
	}
	return p
}

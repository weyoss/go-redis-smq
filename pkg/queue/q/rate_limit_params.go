/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package queue provides message queue operations including CRUD, rate limiting,
// and state management.
//
// This file contains the RateLimitParams type for rate limiting configuration.
// Rate limit operations (Set, Clear, Get) are in queue.go.
package q

import (
	"encoding/json"
	"fmt"
	"time"
)

// RateLimitParams represents rate limiting configuration for a queue.
//
//	{
//	  "limit": 100,
//	  "interval": 60000
//	}
//
// Note: "interval" is serialized as milliseconds.
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
func (p *RateLimitParams) Validate() error {
	if p.limit <= 0 {
		return ErrInvalidRateLimit
	}
	if p.interval < time.Second {
		return ErrInvalidRateLimitInterval
	}
	return nil
}

// Limit returns the maximum number of messages allowed.
func (p *RateLimitParams) Limit() int { return p.limit }

// Interval returns the time window duration.
func (p *RateLimitParams) Interval() time.Duration { return p.interval }

// MarshalJSON implements custom JSON marshaling.
// Serializes interval as milliseconds.
func (p *RateLimitParams) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Limit    int   `json:"limit"`
		Interval int64 `json:"interval"`
	}{
		Limit:    p.limit,
		Interval: p.interval.Milliseconds(),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling.
// Expects interval in milliseconds.
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
func (p *RateLimitParams) String() string {
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

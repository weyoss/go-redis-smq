/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package ratelimit

import (
	"context"
	"encoding/json"
	"fmt"

	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// MarshalRateLimitParams serializes RateLimitParams to a JSON string.
func MarshalRateLimitParams(_ context.Context, rl *publicqueue.RateLimitParams) (string, error) {
	if rl == nil {
		return "", nil
	}

	data, err := json.Marshal(rl)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	return string(data), nil
}

// UnmarshalRateLimitParams deserializes a JSON string back to RateLimitParams.
func UnmarshalRateLimitParams(_ context.Context, data string) (*publicqueue.RateLimitParams, error) {
	if data == "" {
		return nil, nil
	}

	var rl publicqueue.RateLimitParams
	if err := json.Unmarshal([]byte(data), &rl); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	if err := rl.Validate(); err != nil {
		return nil, fmt.Errorf("unmarshal: validate: %w", err)
	}

	return &rl, nil
}

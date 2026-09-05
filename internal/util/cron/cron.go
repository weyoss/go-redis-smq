/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package cron

import (
	"fmt"
	"strings"
	"time"

	"github.com/adhocore/gronx"
)

// ValidateCron checks if a CRON expression is valid.
//
// Supported formats:
//   - 5 fields: minute hour day-of-month month day-of-week (seconds default to 0)
//   - 6 fields: second minute hour day-of-month month day-of-week
//
// The validation is delegated to gronx after checking the field count.
func ValidateCron(expr string) error {
	fields := strings.Fields(expr)

	if len(fields) != 5 && len(fields) != 6 {
		return fmt.Errorf("cron: invalid expression: %s. Only 5 and 6 field expressions are accepted", expr)
	}

	// gronx supports both 5 and 6 field expressions natively.
	if !gronx.IsValid(expr) {
		return fmt.Errorf("cron: invalid expression: %s", expr)
	}

	return nil
}

// ParseCron validates and wraps a CRON expression for scheduling.
func ParseCron(expr string) (*Schedule, error) {
	if err := ValidateCron(expr); err != nil {
		return nil, err
	}
	return &Schedule{expr: expr}, nil
}

// Schedule wraps a validated CRON expression.
type Schedule struct {
	expr string
}

// NextTick returns the next scheduled time after the given time.
// Returns zero time if the expression is empty or no next time is found.
func (s *Schedule) NextTick(after time.Time) time.Time {
	if s.expr == "" {
		return time.Time{}
	}

	next, err := gronx.NextTickAfter(s.expr, after, false)
	if err != nil {
		return time.Time{}
	}
	return next
}

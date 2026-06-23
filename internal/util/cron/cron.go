/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// go/internal/util/cron/cron.go
package cron

import (
	"fmt"
	"strings"
	"time"

	"github.com/adhocore/gronx"
)

// ValidateCron checks if a 5-field CRON expression is valid.
// Format: second minute hour dom month dow
func ValidateCron(expr string) error {
	fields := strings.Fields(expr)
	if len(fields) == 5 {
		expr = "0 " + expr // Add seconds field
	}
	if !gronx.IsValid(expr) {
		return fmt.Errorf("cron: invalid expression: %s", expr)
	}
	return nil
}

// ParseCron validates and wraps a CRON expression for scheduling.
func ParseCron(expr string) (*CronSchedule, error) {
	if err := ValidateCron(expr); err != nil {
		return nil, err
	}
	return &CronSchedule{expr: expr}, nil
}

// CronSchedule wraps a validated CRON expression.
type CronSchedule struct {
	expr string
}

// NextTick returns the next scheduled time after the given time.
// Returns zero time if the expression is empty or no next time is found.
func (s *CronSchedule) NextTick(after time.Time) time.Time {
	if s.expr == "" {
		return time.Time{}
	}

	next, err := gronx.NextTickAfter(s.expr, after, false)
	if err != nil {
		return time.Time{}
	}
	return next
}

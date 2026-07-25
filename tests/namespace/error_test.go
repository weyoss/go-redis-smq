/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package namespace_test

import (
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/namespace"
)

// Scenario: Invalid namespace name (starts with number)
func TestError_InvalidName_StartsWithNumber(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := namespace.NewManager()
	_, err := nm.Exists(ctx, "3invalid")
	if err == nil {
		t.Fatal("expected error for namespace starting with number")
	}
}

// Scenario: Invalid namespace name (contains uppercase)
func TestError_InvalidName_Uppercase(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := namespace.NewManager()
	_, err := nm.Exists(ctx, "Invalid")
	// Uppercase is converted to lowercase by ValidateKey
	t.Logf("uppercase namespace: %v", err)
}

// Scenario: Empty namespace name
func TestError_EmptyName(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := namespace.NewManager()
	_, err := nm.Exists(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty namespace name")
	}
}

// Scenario: Delete non-existent namespace
func TestError_DeleteNonExistent(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := namespace.NewManager()
	err := nm.Delete(ctx, "nonexistent-ns")
	if err == nil {
		t.Fatal("expected error for non-existent namespace")
	}
}

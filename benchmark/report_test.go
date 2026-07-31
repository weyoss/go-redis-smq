/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package benchmark_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestReport prints environment info for benchmark context.
func TestReport(t *testing.T) {
	sep := strings.Repeat("=", 60)
	fmt.Println(sep)
	fmt.Println("RedisSMQ Benchmark Report")
	fmt.Println(sep)
	fmt.Printf("Date: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("OS: %s\n", runtime.GOOS)
	fmt.Printf("Arch: %s\n", runtime.GOARCH)
	fmt.Println(sep)
	fmt.Println()
}

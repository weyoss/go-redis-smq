/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package testutil

import (
	"bufio"
	"io"
	"net"
	"strings"
	"time"
)

// getFreePort returns a free TCP port on localhost.
func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// waitForReady reads from stdout until "Ready to accept connections" is seen or timeout fires.
func waitForReady(stdout io.Reader, timeout time.Duration) bool {
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			// fmt.Printf("testutil: Redis stdout: %s\n", line)
			if strings.Contains(line, "Ready to accept") {
				ready <- true
				return
			}
		}
	}()

	select {
	case <-ready:
		return true
	case <-time.After(timeout):
		return false
	}
}

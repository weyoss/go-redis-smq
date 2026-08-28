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
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	internalredis "github.com/weyoss/go-redis-smq/internal/redis"
)

// RedisProcess manages a real Redis server for integration tests.
type RedisProcess struct {
	cmd    *exec.Cmd
	port   int
	addr   string
	client *goredis.Client
}

// Global lock to ensure only one Redis process is started at a time across all test packages.
var redisStartMu sync.Mutex

// StartRedisProcess starts a Redis server on a random port.
// Uses a global lock to prevent port conflicts when multiple test packages start concurrently.
func StartRedisProcess() (*RedisProcess, error) {
	redisStartMu.Lock()
	defer redisStartMu.Unlock()

	port, err := getFreePort()
	if err != nil {
		return nil, fmt.Errorf("no free port: %w", err)
	}

	redisBinary, err := findOrDownloadRedis()
	if err != nil {
		return nil, fmt.Errorf("redis binary: %w", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("redis-smq-test-%d-%d", os.Getpid(), port))
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	cmd := exec.Command(redisBinary,
		"--port", fmt.Sprintf("%d", port),
		"--save", "",
		"--appendonly", "no",
		"--bind", "127.0.0.1",
		"--dir", dataDir,
		"--dbfilename", "dump.rdb",
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("redis-server pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("redis-server start: %w", err)
	}

	if !waitForReady(stdout, 10*time.Second) {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("redis-server failed to start within timeout")
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	if err := internalredis.Init(context.Background(), client); err != nil {
		_ = client.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("redis init: %w", err)
	}

	return &RedisProcess{
		cmd:    cmd,
		port:   port,
		addr:   addr,
		client: client,
	}, nil
}

// Close stops the Redis server and cleans up the client and temporary files.
func (rp *RedisProcess) Close() {
	// Clear the internal singleton references first.
	internalredis.Close()

	// Close the Redis client we created.
	if rp.client != nil {
		_ = rp.client.Close()
		rp.client = nil
	}

	// Kill the Redis process.
	if rp.cmd != nil && rp.cmd.Process != nil {
		_ = rp.cmd.Process.Kill()
		_ = rp.cmd.Wait()
	}

	// Clean up the data directory.
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("redis-smq-test-%d-%d", os.Getpid(), rp.port))
	_ = os.RemoveAll(dataDir)
}

// Addr returns the Redis server address.
func (rp *RedisProcess) Addr() string {
	return rp.addr
}

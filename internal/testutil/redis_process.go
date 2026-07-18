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

	"github.com/weyoss/go-redis-smq/internal/redis"
)

// RedisProcess manages a real Redis server for integration tests.
type RedisProcess struct {
	cmd  *exec.Cmd
	port int
	addr string
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

	//log.Printf("testutil: finding Redis binary...")
	redisBinary, err := findOrDownloadRedis()
	if err != nil {
		return nil, fmt.Errorf("redis binary: %w", err)
	}
	//log.Printf("testutil: using Redis binary: %s", redisBinary)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	//log.Printf("testutil: starting Redis on %s...", addr)

	// Use a unique data directory per process to avoid conflicts
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("redis-smq-test-%d-%d", os.Getpid(), port))
	os.MkdirAll(dataDir, 0700)

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
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("redis-server pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("redis-server start: %w", err)
	}

	//log.Println("testutil: waiting for Redis to be ready...")
	if !waitForReady(stdout, 10*time.Second) {
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("redis-server failed to start within timeout")
	}
	//log.Println("testutil: Redis is ready")

	ctx := context.Background()
	if err := redis.Init(ctx, redis.Config{Addr: addr}); err != nil {
		cmd.Process.Kill()
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("redis init: %w", err)
	}

	return &RedisProcess{
		cmd:  cmd,
		port: port,
		addr: addr,
	}, nil
}

// Close stops the Redis server and cleans up the client.
func (rp *RedisProcess) Close() {
	//log.Printf("testutil: stopping Redis on %s...", rp.addr)

	// Close the Redis client pool first to release connections.
	redis.Close()

	// Kill the Redis process.
	rp.cmd.Process.Kill()
	rp.cmd.Wait()

	// Clean up the data directory.
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("redis-smq-test-%d-%d", os.Getpid(), rp.port))
	os.RemoveAll(dataDir)

	//log.Println("testutil: Redis stopped")
}

// Addr returns the Redis server address.
func (rp *RedisProcess) Addr() string {
	return rp.addr
}

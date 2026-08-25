/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Consumer crashes while processing – message recovered and consumed by another consumer.
//
// This test uses a real separate process (crash_consumer/main.go) that picks up
// a message and blocks until externally killed.  After the subprocess is killed
// (entire process group), we start a second consumer that will eventually acquire
// the worker lock, detect the dead consumer, recover its in‑flight message, and
// process all three messages.
func TestRecovery_CrashAndRecover(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 120*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-recovery-crash")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 3; i++ {
		if _, err := prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params).SetRetryDelay(0)); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	// Path to the crash consumer program inside testdata.
	crashMain, err := filepath.Abs(filepath.Join("testdata", "crash_consumer", "main.go"))
	if err != nil {
		t.Fatalf("resolve crash consumer path: %v", err)
	}

	redisAddr := redis.Client().Options().Addr

	// Start the crash consumer subprocess with a very short heartbeat TTL (2s)
	// so that it will be detected as dead quickly.
	cmd := exec.Command("go", "run", crashMain)
	cmd.Env = append(os.Environ(),
		"redissmq.ADDR="+redisAddr,
		"QUEUE_NAME="+params.Name(),
		"QUEUE_NS="+params.NS(),
		"HEARTBEAT_TTL=2s",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Create a new process group so we can kill the entire tree.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("start crash consumer: %v", err)
	}

	// Wait until the crash consumer has picked up a message (its processing queue non‑empty).
	time.Sleep(5 * time.Second)
	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	consumerIDs, _ := redis.Client().HKeys(ctx, qKey.Consumers()).Result()
	crashConsumerID := ""
	for _, cid := range consumerIDs {
		length, _ := redis.Client().LLen(ctx, qKey.ConsumerProcessing(cid)).Result()
		if length > 0 {
			crashConsumerID = cid
			break
		}
	}
	if crashConsumerID == "" {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		cmd.Wait()
		t.Fatal("crash consumer did not pick up any messages")
	}
	t.Logf("crash consumer %s has in‑flight message", crashConsumerID)

	// Simulate a real crash by killing the entire process group.
	t.Log("killing crash consumer process group")
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill crash consumer: %v", err)
	}
	cmd.Wait()
	t.Log("crash consumer process group terminated")

	// Consumer B: start it immediately.  It will try to acquire the worker lock,
	// and once it does, its internal reaper will detect the dead consumer and
	// recover the in‑flight message.
	var consumedByB atomic.Int64
	consB := redissmq.NewConsumer()
	consB.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumedByB.Add(1)
		t.Logf("Consumer B received: %s", m.ID)
		return nil
	})
	if err := consB.Run(ctx); err != nil {
		t.Fatalf("run consumer B: %v", err)
	}
	defer consB.Shutdown()

	// The dead consumer's worker lock (TTL 30 s) will expire soon because its
	// refresh loop stopped when we killed the process.  After the lock expires,
	// Consumer B will acquire it and start its reaper (runs every 30 s).
	// We wait up to 70 seconds for the lock to expire, the reaper to run,
	// and all three messages to be consumed.
	deadline := time.After(120 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			pendingLen, _ := redis.Client().LLen(ctx, qKey.Pending()).Result()
			requeuedLen, _ := redis.Client().LLen(ctx, qKey.Requeued()).Result()
			lockKey := qKey.WorkersLock()
			lockExists, _ := redis.Client().Exists(ctx, lockKey).Result()
			t.Fatalf("timeout: Consumer B consumed %d messages (expected 3). pending=%d, requeued=%d, lock-exists=%v",
				consumedByB.Load(), pendingLen, requeuedLen, lockExists > 0)
		case <-ticker.C:
			if consumedByB.Load() >= 3 {
				t.Logf("Consumer B received all 3 messages – crash recovery works")
				return
			}
		}
	}
}

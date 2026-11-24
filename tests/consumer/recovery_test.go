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
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicConsumer "github.com/weyoss/go-redis-smq/pkg/consumer"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// The dead consumer's WorkersLock lease expires after LeaseTTL (5s under
// TestIntervals). Then this Coordinator's reconcile picks up the queue
// within Reconcile (250ms), and the reaper's first tick recovers the
// in-flight message within Reaper (500ms). ~6s total; the deadline below
// gives ~4x slack.
var deadlineDuration = 3*testutil.WorkerIntervals().LeaseTTL +
	testutil.WorkerIntervals().Reconcile +
	testutil.WorkerIntervals().Reaper +
	5*time.Second

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

	// subscribe to messageUnacknowledged BEFORE starting Consumer B.
	// Consumer B's reaper is what fires the event (via
	// ReapConsumers.recoverConsumer), so the subscription must be live
	// before B acquires the worker lock and runs its first reap cycle.
	var mu sync.Mutex
	var unackEvents []publicConsumer.MessageUnacknowledgedPayload
	sub, err := publicConsumer.SubscribeMessageUnacknowledged(
		func(p publicConsumer.MessageUnacknowledgedPayload) {
			mu.Lock()
			unackEvents = append(unackEvents, p)
			mu.Unlock()
		})
	if err != nil {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		cmd.Wait()
		t.Fatalf("subscribe unack: %v", err)
	}
	defer sub.Unsubscribe()

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

	deadline := time.After(deadlineDuration)
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
				// All messages consumed by Consumer B. Now assert H6:
				// the reaper must have emitted exactly one
				// consumer.messageUnacknowledged event for the recovered
				// message, with Cause = CauseOfflineConsumer and
				// ConsumerID = the dead consumer's ID.
				//
				// The crash consumer's handler blocks forever after the
				// first message (see testdata/crash_consumer/main.go —
				// the handler waits on a signal that never arrives), so
				// exactly 1 message was in its ConsumerProcessing list
				// at kill time. That's the count expected below.
				//
				// Give the async event delivery a moment to settle.
				// The publish call itself is synchronous inside the
				// reaper's recoverConsumer; delivery to subscribers is
				// async via Redis pub/sub. The reaper fires before
				// Consumer B is able to dequeue the second message, so
				// by the time we get here the publish has long since
				// occurred — 300ms is generous for delivery.
				time.Sleep(300 * time.Millisecond)

				mu.Lock()
				got := len(unackEvents)
				var causes []int
				var consumerIDs []string
				for _, ev := range unackEvents {
					causes = append(causes, ev.Cause)
					consumerIDs = append(consumerIDs, ev.ConsumerID)
				}
				mu.Unlock()

				if got != 1 {
					t.Fatalf("unack events = %d, want 1 (crash consumer had "+
						"1 in-flight message at kill time). causes: %v, "+
						"consumerIDs: %v", got, causes, consumerIDs)
				}
				if unackEvents[0].Cause != int(publicConsumer.CauseOfflineConsumer) {
					t.Errorf("cause = %d, want CauseOfflineConsumer(%d)",
						unackEvents[0].Cause, publicConsumer.CauseOfflineConsumer)
				}
				if unackEvents[0].ConsumerID != crashConsumerID {
					t.Errorf("consumerID = %q, want crashConsumerID %q",
						unackEvents[0].ConsumerID, crashConsumerID)
				}
				if unackEvents[0].Queue.QueueParams.Name() != params.Name() {
					t.Errorf("queue = %q, want %q",
						unackEvents[0].Queue.QueueParams.Name(), params.Name())
				}
				if unackEvents[0].Queue.QueueParams.NS() != params.NS() {
					t.Errorf("queue ns = %q, want %q",
						unackEvents[0].Queue.QueueParams.NS(), params.NS())
				}

				t.Logf("Consumer B received all 3 messages – crash recovery works; " +
					"reaper emitted 1 unack event with CauseOfflineConsumer")
				return
			}
		}
	}
}

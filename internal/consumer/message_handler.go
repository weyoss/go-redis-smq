/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/lock"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type Handler func(ctx context.Context, m *publicmessage.Transferable) error

type MessageHandler struct {
	mu             sync.RWMutex
	running        bool
	consumerID     string
	queue          *queue.Params
	groupID        string
	handler        Handler
	options        *consumer.Options
	ctx            context.Context
	cancel         context.CancelFunc
	dequeuer       *DequeueMessage
	consumeMsg     *ConsumeMessage
	autoDequeue    bool
	ephemeralGroup bool
	batchAcker     *BatchAcker
	batchUnacker   *BatchUnacker
	errCh          chan error

	// wg tracks the two goroutines that directly call internalredis.Client()
	// and lack recover():
	//   - loop (dequeue + ConsumeMessage)
	//   - acquireWorkerLock (Lock.Acquire / Lock.Release)
	// A nil-pointer panic from internalredis.Close racing either of those
	// would crash the process. Shutdown waits on wg before doing its own
	// cleanup (which includes the last Redis ops in UnsubscribeConsumer /
	// DeleteEphemeralConsumerGroup) — by the time Shutdown returns, neither
	// loop nor acquireWorkerLock is calling Redis.
	//
	// The five background workers spawned by acquireWorkerLock track
	// themselves in their own waitgroups and are additionally waited on via
	// waitWorkers() below.
	wg sync.WaitGroup

	scheduledPublisher    *ScheduledPublisher
	delayedRequeuer       *DelayedRequeuer
	immediateRequeuer     *ImmediateRequeuer
	reapConsumers         *ReapConsumers
	orphanedLockRecoverer *OrphanedLockRecoverer

	workerLock *lock.Lock
	log        *slog.Logger
}

func NewMessageHandler(consumerID string, queue *queue.Params, groupID string, handler Handler, options *consumer.Options) *MessageHandler {
	return &MessageHandler{
		consumerID:  consumerID,
		queue:       queue,
		groupID:     groupID,
		handler:     handler,
		options:     options,
		autoDequeue: true,
		errCh:       make(chan error, 1),
		log:         logger.New("consumer", "handler", consumerID, queue.Name()),
	}
}

func (mh *MessageHandler) Queue() *queue.Params { return mh.queue }

func (mh *MessageHandler) IsRunning() bool {
	mh.mu.RLock()
	defer mh.mu.RUnlock()
	return mh.running
}

func (mh *MessageHandler) Errors() <-chan error {
	return mh.errCh
}

func (mh *MessageHandler) Run(ctx context.Context) error {
	mh.mu.Lock()
	defer mh.mu.Unlock()

	if mh.running {
		mh.log.Debug("already running")
		return nil
	}

	mh.log.Info("starting message handler")

	mh.ctx, mh.cancel = context.WithCancel(ctx)

	effectiveGroupID, err := PrepareConsumerGroup(mh.ctx, mh.consumerID, mh.queue, mh.groupID)
	if err != nil {
		mh.log.Error("failed to prepare consumer group", "error", err)
		return fmt.Errorf("prepare consumer group: %w", err)
	}
	if effectiveGroupID != "" && effectiveGroupID != mh.groupID {
		mh.groupID = effectiveGroupID
		mh.ephemeralGroup = true
		mh.log.Debug("using ephemeral consumer group", "group", effectiveGroupID)
	}

	if err := SubscribeConsumer(mh.ctx, mh.consumerID, mh.queue, mh.groupID); err != nil {
		mh.log.Error("failed to subscribe consumer", "error", err)
		return fmt.Errorf("subscribe: %w", err)
	}
	mh.log.Debug("subscribed to queue")

	mh.batchAcker = NewBatchAcker(mh.queue, mh.consumerID, mh.options.BatchAcks)
	mh.batchAcker.Run(mh.ctx)
	mh.log.Debug("batch acker started", "enabled", mh.options.BatchAcks.Enabled)

	mh.batchUnacker = NewBatchUnacker(mh.queue, mh.groupID, mh.consumerID, mh.options.BatchUnacks)
	mh.batchUnacker.Run(mh.ctx)
	mh.log.Debug("batch unacker started", "enabled", mh.options.BatchUnacks.Enabled)

	mh.dequeuer = NewDequeueMessage(mh.queue, mh.groupID, mh.consumerID)
	if err := mh.dequeuer.Init(mh.ctx); err != nil {
		mh.log.Error("failed to initialize dequeuer", "error", err)
		return fmt.Errorf("dequeue init: %w", err)
	}

	mh.consumeMsg = NewConsumeMessage(mh.queue, mh.groupID, mh.consumerID, mh.handler, mh.batchAcker, mh.batchUnacker)

	qKey := keys.Queue{Namespace: mh.queue.NS(), Name: mh.queue.Name()}
	mh.workerLock = lock.New(
		qKey.WorkersLock(),
		mh.consumerID,
		lock.WithTTL(30*time.Second),
		lock.WithAutoRefresh(10*time.Second),
		lock.WithRetry(lock.ExponentialBackoff(time.Second, 0)),
	)

	// acquireWorkerLock is tracked by wg. It calls workerLock.Acquire /
	// workerLock.Release (Redis) and has no recover(). If it's mid-Redis
	// when the shared client is nilled, the panic propagates out of the
	// unrecovered goroutine and kills the process. Shutdown waits on wg
	// before returning, so by the time redissmq.Shutdown reaches
	// internalredis.Close, acquireWorkerLock has exited.
	mh.wg.Add(1)
	go func() {
		defer mh.wg.Done()
		mh.acquireWorkerLock()
	}()

	mh.running = true

	if mh.autoDequeue {
		mh.wg.Add(1)
		go func() {
			defer mh.wg.Done()
			mh.loop()
		}()
	}

	mh.log.Info("message handler started",
		"queue", mh.queue.String(),
		"group", mh.groupID,
		"ephemeralGroup", mh.ephemeralGroup,
	)

	return nil
}

func (mh *MessageHandler) acquireWorkerLock() {
	mh.log.Debug("attempting to acquire worker lock")
	if err := mh.workerLock.Acquire(mh.ctx); err != nil {
		mh.log.Error("failed to acquire worker lock", "error", err)
		return
	}

	mh.mu.Lock()
	defer mh.mu.Unlock()

	if !mh.running {
		mh.log.Debug("handler stopped before lock acquired — releasing")
		if err := mh.workerLock.Release(context.Background()); err != nil {
			mh.log.Error("failed to release worker lock after stop", "error", err)
		}
		return
	}

	mh.log.Info("worker lock acquired — starting background workers")

	mh.scheduledPublisher = NewScheduledPublisher(mh.queue, mh.groupID, mh.consumerID)
	mh.scheduledPublisher.Run(mh.ctx)

	mh.delayedRequeuer = NewDelayedRequeuer(mh.queue, mh.groupID, mh.consumerID)
	mh.delayedRequeuer.Run(mh.ctx)

	mh.immediateRequeuer = NewImmediateRequeuer(mh.queue, mh.groupID, mh.consumerID)
	mh.immediateRequeuer.Run(mh.ctx)

	mh.reapConsumers = NewReapConsumers(mh.queue, mh.groupID, mh.consumerID)
	mh.reapConsumers.Run(mh.ctx)

	mh.orphanedLockRecoverer = NewOrphanedLockRecoverer(mh.queue, mh.consumerID)
	mh.orphanedLockRecoverer.Run(mh.ctx)

	mh.log.Debug("all background workers started")
}

// Shutdown stops the handler from an external caller (the runner).
//
// It cancels the handler context, waits for the dequeue/consume loop and
// for acquireWorkerLock to exit, waits for the five background workers
// (spawned by acquireWorkerLock), then shuts down the batchers, releases
// the worker lock, and unsubscribes.
//
// The waits guarantee no goroutine spawned by this handler is calling
// internalredis.Client() after Shutdown returns — which is what prevents
// internalredis.Close (called later by redissmq.Shutdown) from racing a
// live Redis op and panicking in an unrecovered goroutine.
//
// Shutdown blocks until the in-flight handler returns. Handlers must respect
// the context passed to them — a handler that ignores ctx and blocks forever
// will cause Shutdown to block forever. The library does not impose a
// shutdown timeout: the alternative (giving up and shutting down the batchers
// anyway) silently reintroduces the corruption window, which is worse than
// hanging. If a handler needs a bound, it should enforce one itself via the
// context (e.g. select on ctx.Done()).
//
// Safe to call multiple times and from any goroutine.
func (mh *MessageHandler) Shutdown() {
	mh.beginShutdown(true)
}

// selfShutdown is called from inside the loop goroutine (on queue state
// changes). It cannot wait on mh.wg — that would deadlock against the
// goroutine it is called from.
func (mh *MessageHandler) selfShutdown() {
	mh.beginShutdown(false)
}

// beginShutdown transitions running→false under the lock, then performs the
// actual shutdown work without holding the lock.
//
// waitForLoop must be true when the caller is NOT the loop goroutine (so we
// can safely wait for it to exit) and false when called from loop (so we
// don't wait on ourselves).
func (mh *MessageHandler) beginShutdown(waitForLoop bool) {
	mh.mu.Lock()

	if !mh.running {
		// Another shutdown (selfShutdown from loop, or a concurrent external
		// Shutdown) already started cleanup. If we're external, wait for it
		// to finish — selfShutdown is on the loop goroutine (part of wg), so
		// wg.Wait() blocks until its cleanup (UnsubscribeConsumer,
		// DeleteEphemeralConsumerGroup) completes. Then wait on the five
		// background workers, which selfShutdown does not wait for.
		mh.mu.Unlock()
		if waitForLoop {
			mh.wg.Wait()
		}
		mh.waitWorkers()
		mh.log.Debug("shutdown called but not running")
		return
	}

	mh.running = false
	cancel := mh.cancel
	batchAcker := mh.batchAcker
	batchUnacker := mh.batchUnacker
	workerLock := mh.workerLock
	ephemeralGroup := mh.ephemeralGroup
	groupID := mh.groupID
	mh.mu.Unlock()

	mh.log.Info("shutting down message handler")

	if cancel != nil {
		cancel()
	}

	if waitForLoop {
		// Wait for the two directly-tracked goroutines (loop and
		// acquireWorkerLock) to exit. loop is the only code path that
		// writes to the batchers; acquireWorkerLock is the only code
		// path that spawns the five workers. After this returns, no more
		// writes to the batchers and no more worker spawns are possible.
		mh.wg.Wait()
	}

	// Wait for the five background workers. They're independent goroutines
	// that do not call back into the handler, so this is safe from either
	// branch (including selfShutdown on loop). Re-captures the pointers
	// under mh.mu in case acquireWorkerLock spawned them during wg.Wait().
	mh.waitWorkers()

	if batchAcker != nil {
		mh.log.Debug("shutting down batch acker")
		batchAcker.Shutdown()
	}
	if batchUnacker != nil {
		mh.log.Debug("shutting down batch unacker")
		batchUnacker.Shutdown()
	}

	if workerLock != nil {
		mh.log.Debug("releasing worker lock")
		if err := workerLock.Release(context.Background()); err != nil {
			mh.log.Error("failed to release worker lock", "error", err)
		}
	}

	mh.log.Debug("unsubscribing consumer")
	if err := UnsubscribeConsumer(context.Background(), mh.consumerID, mh.queue, groupID); err != nil {
		mh.log.Error("failed to unsubscribe consumer", "error", err)
	}

	if ephemeralGroup {
		mh.log.Debug("deleting ephemeral consumer group", "group", groupID)
		if err := DeleteEphemeralConsumerGroup(context.Background(), mh.consumerID, mh.queue, groupID); err != nil {
			mh.log.Error("failed to delete ephemeral consumer group", "error", err)
		}
	}

	mh.log.Info("message handler shut down complete")
}

// waitWorkers waits for the five background workers spawned by
// acquireWorkerLock to exit.
//
// Re-captures the worker pointers under mh.mu on each call. Why: if
// acquireWorkerLock hadn't finished Acquire() when the initial capture
// happened (in beginShutdown), the pointers were nil at that moment. The
// subsequent mh.wg.Wait() unblocks acquireWorkerLock to either spawn the
// workers (success path) or return without spawning (ctx cancelled path).
// Re-capturing after the wait picks up whatever got spawned.
//
// Safe from any goroutine — workers don't call back into the handler.
// Multiple concurrent calls (e.g. external Shutdown racing selfShutdown)
// are safe — wg.Wait() on the same worker is not a data race, and the
// extra wait is a no-op once the worker has exited.
func (mh *MessageHandler) waitWorkers() {
	mh.mu.RLock()
	sp := mh.scheduledPublisher
	dr := mh.delayedRequeuer
	ir := mh.immediateRequeuer
	rc := mh.reapConsumers
	olr := mh.orphanedLockRecoverer
	mh.mu.RUnlock()

	if sp != nil {
		sp.Wait()
	}
	if dr != nil {
		dr.Wait()
	}
	if ir != nil {
		ir.Wait()
	}
	if rc != nil {
		rc.Wait()
	}
	if olr != nil {
		olr.Wait()
	}
}

func (mh *MessageHandler) loop() {
	mh.log.Debug("starting dequeue loop")
	defer close(mh.errCh)
	defer mh.log.Debug("dequeue loop stopped")

	for {
		select {
		case <-mh.ctx.Done():
			return
		default:
		}

		envelope, err := mh.dequeuer.Dequeue(mh.ctx)
		if err != nil {
			if errors.Is(err, consumer.ErrQueueStopped) ||
				errors.Is(err, consumer.ErrQueueLocked) ||
				errors.Is(err, consumer.ErrQueueInvalidState) {
				mh.log.Warn("queue state changed — stopping handler", "error", err)
				mh.errCh <- fmt.Errorf("handler stopped: %w", err)
				// selfShutdown (not Shutdown): we are inside the loop goroutine,
				// so beginShutdown must not wait on mh.wg — that would deadlock.
				mh.selfShutdown()
				return
			}
			mh.log.Error("dequeue error", "error", err)
			select {
			case <-mh.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if envelope == nil {
			select {
			case <-mh.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		mh.consumeMsg.Consume(mh.ctx, envelope)
	}
}

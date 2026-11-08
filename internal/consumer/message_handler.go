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
	// and lack recover(): loop and acquireWorkerLock. Shutdown waits on wg
	// before doing its own cleanup so no goroutine spawned by this handler
	// is calling Redis after internalredis.Close().
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

// Run initializes and starts the handler.
//
// Run is transactional: on any failure, rollbackLocked unwinds whatever
// partial state was created (derived context, ephemeral consumer group,
// Redis subscription, batch ackers, worker lock) before returning the
// error. The handler is left in the same state it was in before Run was
// called, and a subsequent Run with corrected parameters can proceed
// cleanly — no residue, no "already running" surprises.
//
// Rollback runs synchronously under mh.mu. This is safe because the
// fallible steps all complete before any goroutine that depends on the
// initialized state is spawned. The wg-tracked goroutines (acquireWorkerLock,
// loop) and the five background workers are spawned only in the tail of
// Run, after every fallible step has succeeded.
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
		mh.rollbackLocked()
		return fmt.Errorf("prepare consumer group: %w", err)
	}
	if effectiveGroupID != "" && effectiveGroupID != mh.groupID {
		mh.groupID = effectiveGroupID
		mh.ephemeralGroup = true
		mh.log.Debug("using ephemeral consumer group", "group", effectiveGroupID)
	}

	if err := SubscribeConsumer(mh.ctx, mh.consumerID, mh.queue, mh.groupID); err != nil {
		mh.log.Error("failed to subscribe consumer", "error", err)
		mh.rollbackLocked()
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
		mh.rollbackLocked()
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

	// Mark running before spawning any worker. Semantically "we are running"
	// is a precondition for the goroutines that depend on that state to
	// exist. Run holds mh.mu throughout, so the workers can't observe this
	// until they acquire the lock — which happens only after Run returns.
	mh.running = true

	// acquireWorkerLock is tracked by wg. It calls workerLock.Acquire /
	// workerLock.Release (Redis) and has no recover(); a nil-client panic
	// from a concurrent internalredis.Close would crash the process.
	// beginShutdown's wg.Wait covers it.
	mh.wg.Add(1)
	go func() {
		defer mh.wg.Done()
		mh.acquireWorkerLock()
	}()

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

// rollbackLocked unwinds partial initialization after a failed Run.
//
// The caller MUST hold mh.mu. No goroutines spawned by Run are running yet
// (acquireWorkerLock and loop are spawned only after every fallible step
// succeeds), so this is fully synchronous — no wg.Wait, no waiting on
// workers. Shares the cleanup steps with beginShutdown (same nil-safe,
// idempotent operations, same order), but is simpler because there are no
// concurrent workers to coordinate with.
//
// Idempotent: every step is nil-safe, so calling twice is harmless. In
// practice it's called once per failed Run, but the discipline matches
// beginShutdown.
func (mh *MessageHandler) rollbackLocked() {
	if mh.cancel != nil {
		mh.cancel()
	}

	if mh.batchAcker != nil {
		mh.batchAcker.Shutdown()
	}
	if mh.batchUnacker != nil {
		mh.batchUnacker.Shutdown()
	}

	if mh.workerLock != nil {
		if err := mh.workerLock.Release(context.Background()); err != nil {
			mh.log.Error("failed to release worker lock after rollback", "error", err)
		}
	}

	// UnsubscribeConsumer is idempotent — succeeds even if the consumer
	// was never registered (unsubscribe-consumer.lua's HDEL/SREM/DEL on
	// missing keys are all no-ops; the LLEN check on a missing
	// ConsumerProcessing list returns 0). Safe to call unconditionally,
	// even when the failure was at PrepareConsumerGroup before
	// SubscribeConsumer ever ran.
	if err := UnsubscribeConsumer(context.Background(), mh.consumerID, mh.queue, mh.groupID); err != nil {
		mh.log.Error("failed to unsubscribe consumer after rollback", "error", err)
	}

	if mh.ephemeralGroup {
		if err := DeleteEphemeralConsumerGroup(context.Background(), mh.consumerID, mh.queue, mh.groupID); err != nil {
			mh.log.Error("failed to delete ephemeral consumer group after rollback", "error", err)
		}
	}

	// Reset every field so a subsequent Run (or a caller inspecting the
	// discarded mh) sees the pre-Run shape, not a partial init.
	mh.batchAcker = nil
	mh.batchUnacker = nil
	mh.workerLock = nil
	mh.dequeuer = nil
	mh.consumeMsg = nil
	mh.cancel = nil
	mh.ctx = nil
	mh.ephemeralGroup = false
	mh.running = false
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

// Shutdown, selfShutdown, beginShutdown, waitWorkers, loop — unchanged from
// PR 1. Reproduced here for completeness; see PR 1's message_handler.go
// for the reasoning behind each.

func (mh *MessageHandler) Shutdown() {
	mh.beginShutdown(true)
}

func (mh *MessageHandler) selfShutdown() {
	mh.beginShutdown(false)
}

func (mh *MessageHandler) beginShutdown(waitForLoop bool) {
	mh.mu.Lock()

	if !mh.running {
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
		mh.wg.Wait()
	}

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

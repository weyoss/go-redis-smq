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

	go mh.acquireWorkerLock()

	mh.running = true

	if mh.autoDequeue {
		go mh.loop()
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

func (mh *MessageHandler) Shutdown() {
	mh.mu.Lock()
	defer mh.mu.Unlock()
	mh.shutdownLocked()
}

func (mh *MessageHandler) shutdownLocked() {
	if !mh.running {
		mh.log.Debug("shutdown called but not running")
		return
	}
	mh.running = false

	mh.log.Info("shutting down message handler")

	if mh.cancel != nil {
		mh.cancel()
	}

	if mh.batchAcker != nil {
		mh.log.Debug("shutting down batch acker")
		mh.batchAcker.Shutdown()
	}
	if mh.batchUnacker != nil {
		mh.log.Debug("shutting down batch unacker")
		mh.batchUnacker.Shutdown()
	}

	if mh.workerLock != nil {
		mh.log.Debug("releasing worker lock")
		if err := mh.workerLock.Release(context.Background()); err != nil {
			mh.log.Error("failed to release worker lock", "error", err)
		}
	}

	mh.log.Debug("unsubscribing consumer")
	if err := UnsubscribeConsumer(
		context.Background(), mh.consumerID, mh.queue, mh.groupID,
	); err != nil {
		mh.log.Error("failed to unsubscribe consumer", "error", err)
	}

	if mh.ephemeralGroup {
		mh.log.Debug("deleting ephemeral consumer group", "group", mh.groupID)
		if err := DeleteEphemeralConsumerGroup(
			context.Background(), mh.consumerID, mh.queue, mh.groupID,
		); err != nil {
			mh.log.Error("failed to delete ephemeral consumer group", "error", err)
		}
	}

	mh.log.Info("message handler shut down complete")
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
			if errors.Is(err, consumer.ErrQueueStopped) || errors.Is(err, consumer.ErrQueueLocked) || errors.Is(err, consumer.ErrQueueInvalidState) {
				mh.log.Warn("queue state changed — stopping handler", "error", err)
				mh.errCh <- fmt.Errorf("handler stopped: %w", err)
				mh.Shutdown()
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

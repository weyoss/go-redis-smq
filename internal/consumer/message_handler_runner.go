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
	"fmt"
	"log/slog"
	"sync"
	"time"

	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	publicconsumer "github.com/weyoss/go-redis-smq/pkg/consumer"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

type MessageHandlerRunner struct {
	mu           sync.RWMutex
	consumerID   string
	options      *publicconsumer.Options
	handlers     []*handlerConfig
	instances    []*MessageHandler
	stateTracker *QueueStateTracker
	ctx          context.Context
	cancel       context.CancelFunc

	// closed is set under r.mu at the start of Shutdown, and cleared under
	// r.mu at the start of a fresh Run. It gates the spawn of reconcileLoop
	// in Run against the reconcileWg.Wait in Shutdown — without it, a
	// concurrent Shutdown between Run's r.mu release and the spawn would
	// observe reconcileWg == 0 (wait returns immediately), and the
	// reconcileLoop would run unsupervised after Shutdown returns,
	// potentially calling Redis after internalredis.Close nils the client.
	closed bool

	// reconcileWg tracks the reconcileLoop goroutine. reconcile can be in
	// the middle of mh_new.Run or mh_new.Shutdown (both Redis-touching) when
	// Shutdown fires; Shutdown waits on reconcileWg (outside r.mu) before
	// returning, so no goroutine spawned by the runner is still calling
	// internalredis.Client() when redissmq.Shutdown reaches internalredis.Close.
	reconcileWg sync.WaitGroup

	log *slog.Logger
}

type handlerConfig struct {
	queue   *publicqueue.Params
	groupID string
	handler Handler
}

func NewMessageHandlerRunner(consumerID string, options *publicconsumer.Options) *MessageHandlerRunner {
	return &MessageHandlerRunner{
		consumerID: consumerID,
		options:    options,
		log:        logger.New("consumer", "runner", consumerID),
	}
}

func (r *MessageHandlerRunner) AddHandler(queue *publicqueue.Params, groupID string, handler Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := queue.String()
	for i, h := range r.handlers {
		if h.queue.String() == key && h.groupID == groupID {
			r.log.Debug("replacing handler", "queue", key, "group", groupID)
			for j, inst := range r.instances {
				if inst.queue.String() == key && inst.groupID == groupID {
					inst.Shutdown()
					r.instances = append(r.instances[:j], r.instances[j+1:]...)
					break
				}
			}
			r.handlers[i] = &handlerConfig{queue: queue, groupID: groupID, handler: handler}
			return
		}
	}

	r.log.Debug("adding handler", "queue", key, "group", groupID)
	r.handlers = append(r.handlers, &handlerConfig{
		queue:   queue,
		groupID: groupID,
		handler: handler,
	})
}

func (r *MessageHandlerRunner) RemoveHandler(queue *publicqueue.Params, groupID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := queue.String()
	r.log.Debug("removing handler", "queue", key, "group", groupID)

	for i, h := range r.handlers {
		if h.queue.String() == key && h.groupID == groupID {
			r.handlers = append(r.handlers[:i], r.handlers[i+1:]...)
			break
		}
	}

	for i, inst := range r.instances {
		if inst.queue.String() == key && inst.groupID == groupID {
			inst.Shutdown()
			r.instances = append(r.instances[:i], r.instances[i+1:]...)
			break
		}
	}
}

func (r *MessageHandlerRunner) HasHandlers() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers) > 0
}

func (r *MessageHandlerRunner) Run(ctx context.Context) error {
	r.mu.Lock()
	if r.ctx != nil {
		r.mu.Unlock()
		return nil
	}
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.closed = false
	runnerCtx := r.ctx
	configs := make([]*handlerConfig, len(r.handlers))
	copy(configs, r.handlers)
	r.mu.Unlock()

	r.log.Info("starting message handler runner", "handlers", len(configs))

	tracker := NewQueueStateTracker(
		r.onQueueStopped,
		r.onQueuePaused,
		r.onQueueLocked,
		r.onQueueActive,
	)
	r.mu.Lock()
	r.stateTracker = tracker
	r.mu.Unlock()

	// Note: r.mu is NOT held for the duration of this loop. A concurrent
	// Shutdown can cancel runnerCtx and set r.instances = nil mid-iteration.
	// The runnerCtx.Err() checks below ensure Run aborts cleanly in that case
	// rather than orphaning a MessageHandler started on a cancelled context.
	started := 0
	for _, cfg := range configs {
		if err := runnerCtx.Err(); err != nil {
			r.Shutdown()
			return fmt.Errorf("consumer: shutdown during startup: %w", err)
		}

		if !r.isQueueActive(runnerCtx, cfg.queue) {
			r.log.Warn("skipping inactive queue", "queue", cfg.queue.String())
			continue
		}

		r.log.Debug("starting message handler", "queue", cfg.queue.String(), "group", cfg.groupID)
		mh := NewMessageHandler(r.consumerID, cfg.queue, cfg.groupID, cfg.handler, r.options)
		if err := mh.Run(runnerCtx); err != nil {
			// Distinguish cancellation (shutdown raced us) from real failure
			// — avoids noisy "failed to start" logs during graceful shutdown.
			if cerr := runnerCtx.Err(); cerr != nil {
				r.log.Debug("message handler start aborted by shutdown",
					"queue", cfg.queue.String(), "group", cfg.groupID)
				r.Shutdown()
				return fmt.Errorf("consumer: shutdown during startup: %w", cerr)
			}
			r.log.Error("failed to start message handler",
				"queue", cfg.queue.String(),
				"group", cfg.groupID,
				"error", err,
			)
			r.Shutdown()
			return err
		}
		r.mu.Lock()
		if cerr := runnerCtx.Err(); cerr != nil {
			r.mu.Unlock()
			mh.Shutdown()
			r.Shutdown()
			return fmt.Errorf("consumer: shutdown during startup: %w", cerr)
		}
		r.instances = append(r.instances, mh)
		r.mu.Unlock()
		started++
	}

	if started == 0 && len(configs) > 0 {
		r.log.Error("all queues are non-operational")
		r.Shutdown()
		return fmt.Errorf("consumer: all queues are non-operational")
	}

	r.log.Info("message handler runner started", "active", started, "total", len(configs))

	// Spawn reconcileLoop under r.mu with a closed guard. Without the guard:
	// a concurrent Shutdown between our r.mu release and this spawn would
	// observe reconcileWg counter 0 (its Wait returns immediately), and our
	// Add(1) would run after — the goroutine would proceed unsupervised after
	// Shutdown returned and could call Redis after internalredis.Close.
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.reconcileWg.Add(1)
	go func() {
		defer r.reconcileWg.Done()
		r.reconcileLoop(runnerCtx)
	}()
	r.mu.Unlock()

	return nil
}

// Shutdown stops the runner. Safe to call multiple times.
//
// Restructured from the previous shutdownLocked form: it must NOT hold r.mu
// across reconcileWg.Wait — reconcile acquires r.mu in its C8 under-lock
// ctx.Err check, so holding r.mu while waiting on reconcileWg would
// deadlock. The lock is held only briefly for state capture and clearing.
//
// Also must NOT hold r.mu across the mh.Shutdown() calls: each can block on
// mh.wg.Wait() for up to the handler's runtime (see C7), and holding the
// runner lock there would stall StopHandler / StartHandler / other
// reconcile operations.
func (r *MessageHandlerRunner) Shutdown() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()

	// Wait for reconcileLoop to exit (including any in-flight mh_new.Run
	// or mh_new.Shutdown it may have started). After this returns, no
	// goroutine spawned by the runner can call internalredis.Client().
	r.reconcileWg.Wait()

	r.mu.Lock()
	instances := r.instances
	r.instances = nil
	tracker := r.stateTracker
	r.stateTracker = nil
	r.cancel = nil
	r.ctx = nil
	r.mu.Unlock()

	for _, mh := range instances {
		mh.Shutdown()
	}
	if tracker != nil {
		tracker.Shutdown()
	}
}

func (r *MessageHandlerRunner) StopHandler(queue *publicqueue.Params, groupID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := queue.String()
	r.log.Debug("stopping handler", "queue", key, "group", groupID)

	for i, inst := range r.instances {
		if inst.queue.String() == key && inst.groupID == groupID {
			inst.Shutdown()
			r.instances = append(r.instances[:i], r.instances[i+1:]...)
			return true
		}
	}
	return false
}

func (r *MessageHandlerRunner) StartHandler(queue *publicqueue.Params, groupID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := queue.String()

	var cfg *handlerConfig
	for _, h := range r.handlers {
		if h.queue.String() == key && h.groupID == groupID {
			cfg = h
			break
		}
	}
	if cfg == nil {
		r.log.Debug("no handler config found to start", "queue", key, "group", groupID)
		return false
	}

	for _, inst := range r.instances {
		if inst.queue.String() == key && inst.groupID == groupID {
			r.log.Debug("handler already running", "queue", key, "group", groupID)
			return false
		}
	}

	r.log.Debug("starting handler", "queue", key, "group", groupID)
	mh := NewMessageHandler(r.consumerID, cfg.queue, cfg.groupID, cfg.handler, r.options)
	if err := mh.Run(r.ctx); err != nil {
		r.log.Error("failed to start handler", "queue", key, "group", groupID, "error", err)
		return false
	}
	r.instances = append(r.instances, mh)
	return true
}

func (r *MessageHandlerRunner) Queues() []*publicqueue.Params {
	r.mu.RLock()
	defer r.mu.RUnlock()

	queues := make([]*publicqueue.Params, 0, len(r.handlers))
	for _, h := range r.handlers {
		queues = append(queues, h.queue)
	}
	return queues
}

func (r *MessageHandlerRunner) HandlerCount() (total, active, stopped, paused, locked int) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	total = len(r.handlers)
	for _, h := range r.handlers {
		switch {
		case r.stateTracker.IsActive(h.queue):
			active++
		case r.stateTracker.IsStopped(h.queue):
			stopped++
		case r.stateTracker.IsPaused(h.queue):
			paused++
		case r.stateTracker.IsLocked(h.queue):
			locked++
		}
	}
	return
}

// isQueueActive checks the queue operational state using the internal queue manager.
func (r *MessageHandlerRunner) isQueueActive(ctx context.Context, queue *publicqueue.Params) bool {
	props, err := internalqueue.NewManager().Store().Load(ctx, queue)
	if err != nil {
		r.log.Debug("failed to get queue properties", "queue", queue.String(), "error", err)
		return false
	}
	return props.OperationalState == publicqueue.StateActive
}

// ── Queue state change callbacks ──

func (r *MessageHandlerRunner) onQueueStopped(queue *publicqueue.Params) {
	r.log.Warn("queue stopped — stopping handlers", "queue", queue.String())
	r.mu.RLock()
	var toStop []struct {
		queue   *publicqueue.Params
		groupID string
	}
	for _, cfg := range r.handlers {
		if cfg.queue.String() == queue.String() {
			toStop = append(toStop, struct {
				queue   *publicqueue.Params
				groupID string
			}{cfg.queue, cfg.groupID})
		}
	}
	r.mu.RUnlock()

	for _, s := range toStop {
		r.StopHandler(s.queue, s.groupID)
	}
}

func (r *MessageHandlerRunner) onQueuePaused(queue *publicqueue.Params) {
	r.log.Warn("queue paused — stopping handlers", "queue", queue.String())
	r.mu.RLock()
	var toStop []struct {
		queue   *publicqueue.Params
		groupID string
	}
	for _, cfg := range r.handlers {
		if cfg.queue.String() == queue.String() {
			toStop = append(toStop, struct {
				queue   *publicqueue.Params
				groupID string
			}{cfg.queue, cfg.groupID})
		}
	}
	r.mu.RUnlock()

	for _, s := range toStop {
		r.StopHandler(s.queue, s.groupID)
	}
}

func (r *MessageHandlerRunner) onQueueLocked(queue *publicqueue.Params) {
	r.log.Warn("queue locked — stopping handlers", "queue", queue.String())
	r.mu.RLock()
	var toStop []struct {
		queue   *publicqueue.Params
		groupID string
	}
	for _, cfg := range r.handlers {
		if cfg.queue.String() == queue.String() {
			toStop = append(toStop, struct {
				queue   *publicqueue.Params
				groupID string
			}{cfg.queue, cfg.groupID})
		}
	}
	r.mu.RUnlock()

	for _, s := range toStop {
		r.StopHandler(s.queue, s.groupID)
	}
}

func (r *MessageHandlerRunner) onQueueActive(queue *publicqueue.Params) {
	r.log.Info("queue active — starting handlers", "queue", queue.String())
	r.mu.RLock()
	var toStart []struct {
		queue   *publicqueue.Params
		groupID string
	}
	for _, cfg := range r.handlers {
		if cfg.queue.String() == queue.String() {
			toStart = append(toStart, struct {
				queue   *publicqueue.Params
				groupID string
			}{cfg.queue, cfg.groupID})
		}
	}
	r.mu.RUnlock()

	for _, s := range toStart {
		r.StartHandler(s.queue, s.groupID)
	}
}

// ── Supervisor reconciliation ──

func (r *MessageHandlerRunner) reconcileLoop(ctx context.Context) {
	r.log.Debug("starting reconciliation loop")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Debug("reconciliation loop stopped")
			return
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

// reconcile restarts any handler that should be running but isn't (e.g. the
// queue was paused and resumed, and the state-change callback missed the
// window). It takes ctx explicitly so cancellation is observable without
// reading r.ctx.
//
// reconcile releases r.mu between reading the handler list and appending a
// freshly-started handler. Shutdown acquires r.mu in that window, cancels
// ctx, and clears r.instances — after which appending would orphan the new
// MessageHandler (subscribed, heartbeat, worker lock, no tracker). The
// ctx.Err() checks below, especially the one under r.mu before the append,
// close that window: the check+append is atomic with respect to Shutdown's
// clear, so either Shutdown runs first (we see cancellation and abort) or we
// run first (Shutdown will shut our handler down via r.instances).
func (r *MessageHandlerRunner) reconcile(ctx context.Context) {
	r.mu.RLock()
	var toStart []*handlerConfig
	for _, cfg := range r.handlers {
		if !r.stateTracker.IsActive(cfg.queue) {
			continue
		}
		key := cfg.queue.String()
		running := false
		for _, inst := range r.instances {
			if inst.queue.String() == key && inst.groupID == cfg.groupID && inst.IsRunning() {
				running = true
				break
			}
		}
		if !running {
			toStart = append(toStart, cfg)
		}
	}
	r.mu.RUnlock()

	if len(toStart) == 0 {
		return
	}

	r.log.Info("reconciling — restarting handlers", "count", len(toStart))

	for _, cfg := range toStart {
		if err := ctx.Err(); err != nil {
			r.log.Debug("reconcile: shutting down — aborting before next handler")
			return
		}

		r.log.Debug("restarting handler", "queue", cfg.queue.String(), "group", cfg.groupID)
		mh := NewMessageHandler(r.consumerID, cfg.queue, cfg.groupID, cfg.handler, r.options)
		if err := mh.Run(ctx); err != nil {
			if cerr := ctx.Err(); cerr != nil {
				r.log.Debug("reconcile: handler start aborted by shutdown",
					"queue", cfg.queue.String(), "group", cfg.groupID)
				return
			}
			r.log.Error("failed to restart handler",
				"queue", cfg.queue.String(),
				"group", cfg.groupID,
				"error", err,
			)
			continue
		}

		// Check cancellation under r.mu immediately before appending. If
		// Shutdown cleared r.instances in the window between mh.Run
		// succeeding and this lock, we must not append — the new handler
		// would be untracked and never shut down. Shut it down explicitly
		// (without holding r.mu — mh.Shutdown may block on mh.wg.Wait()).
		r.mu.Lock()
		if cerr := ctx.Err(); cerr != nil {
			r.mu.Unlock()
			mh.Shutdown()
			r.log.Debug("reconcile: handler started after shutdown — shut down explicitly",
				"queue", cfg.queue.String(), "group", cfg.groupID)
			return
		}
		r.instances = append(r.instances, mh)
		r.mu.Unlock()
	}
}

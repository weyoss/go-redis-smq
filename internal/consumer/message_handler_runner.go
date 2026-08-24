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

	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	pubQueue "github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

type MessageHandlerRunner struct {
	mu           sync.RWMutex
	consumerID   string
	options      *consumer.Options
	handlers     []*handlerConfig
	instances    []*MessageHandler
	stateTracker *QueueStateTracker
	ctx          context.Context
	cancel       context.CancelFunc
	log          *slog.Logger
}

type handlerConfig struct {
	queue   *q.QueueParams
	groupID string
	handler Handler
}

func NewMessageHandlerRunner(consumerID string, options *consumer.Options) *MessageHandlerRunner {
	return &MessageHandlerRunner{
		consumerID: consumerID,
		options:    options,
		log:        logger.New("consumer", "runner", consumerID),
	}
}

func (r *MessageHandlerRunner) AddHandler(queue *q.QueueParams, groupID string, handler Handler) {
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

func (r *MessageHandlerRunner) RemoveHandler(queue *q.QueueParams, groupID string) {
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
	r.ctx, r.cancel = context.WithCancel(ctx)
	configs := make([]*handlerConfig, len(r.handlers))
	copy(configs, r.handlers)
	r.mu.Unlock()

	r.log.Info("starting message handler runner", "handlers", len(configs))

	r.stateTracker = NewQueueStateTracker(
		r.onQueueStopped,
		r.onQueuePaused,
		r.onQueueLocked,
		r.onQueueActive,
	)

	started := 0
	for _, cfg := range configs {
		if !r.isQueueActive(ctx, cfg.queue) {
			r.log.Warn("skipping inactive queue", "queue", cfg.queue.String())
			continue
		}

		r.log.Debug("starting message handler", "queue", cfg.queue.String(), "group", cfg.groupID)
		mh := NewMessageHandler(r.consumerID, cfg.queue, cfg.groupID, cfg.handler, r.options)
		if err := mh.Run(r.ctx); err != nil {
			r.log.Error("failed to start message handler",
				"queue", cfg.queue.String(),
				"group", cfg.groupID,
				"error", err,
			)
			r.shutdownLocked()
			return err
		}
		r.mu.Lock()
		r.instances = append(r.instances, mh)
		r.mu.Unlock()
		started++
	}

	if started == 0 && len(configs) > 0 {
		r.log.Error("all queues are non-operational")
		r.shutdownLocked()
		return fmt.Errorf("consumer: all queues are non-operational")
	}

	r.log.Info("message handler runner started", "active", started, "total", len(configs))

	go r.reconcileLoop()
	return nil
}

func (r *MessageHandlerRunner) Shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shutdownLocked()
}

func (r *MessageHandlerRunner) shutdownLocked() {
	if r.cancel != nil {
		r.cancel()
	}

	r.log.Info("shutting down message handler runner", "instances", len(r.instances))

	for _, mh := range r.instances {
		mh.Shutdown()
	}
	r.instances = nil

	if r.stateTracker != nil {
		r.stateTracker.Shutdown()
	}

	r.log.Debug("message handler runner shut down complete")
}

func (r *MessageHandlerRunner) StopHandler(queue *q.QueueParams, groupID string) bool {
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

func (r *MessageHandlerRunner) StartHandler(queue *q.QueueParams, groupID string) bool {
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

func (r *MessageHandlerRunner) Queues() []*q.QueueParams {
	r.mu.RLock()
	defer r.mu.RUnlock()

	queues := make([]*q.QueueParams, 0, len(r.handlers))
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
		if r.stateTracker.IsActive(h.queue) {
			active++
		} else if r.stateTracker.IsStopped(h.queue) {
			stopped++
		} else if r.stateTracker.IsPaused(h.queue) {
			paused++
		} else if r.stateTracker.IsLocked(h.queue) {
			locked++
		}
	}
	return
}

func (r *MessageHandlerRunner) isQueueActive(ctx context.Context, queue *q.QueueParams) bool {
	props, err := pubQueue.GetQueueProps(ctx, queue)
	if err != nil {
		r.log.Debug("failed to get queue properties", "queue", queue.String(), "error", err)
		return false
	}
	return props.OperationalState == q.StateActive
}

// ── Queue state change callbacks ──

func (r *MessageHandlerRunner) onQueueStopped(queue *q.QueueParams) {
	r.log.Warn("queue stopped — stopping handlers", "queue", queue.String())
	r.mu.RLock()
	var toStop []struct {
		queue   *q.QueueParams
		groupID string
	}
	for _, cfg := range r.handlers {
		if cfg.queue.String() == queue.String() {
			toStop = append(toStop, struct {
				queue   *q.QueueParams
				groupID string
			}{cfg.queue, cfg.groupID})
		}
	}
	r.mu.RUnlock()

	for _, s := range toStop {
		r.StopHandler(s.queue, s.groupID)
	}
}

func (r *MessageHandlerRunner) onQueuePaused(queue *q.QueueParams) {
	r.log.Warn("queue paused — stopping handlers", "queue", queue.String())
	r.mu.RLock()
	var toStop []struct {
		queue   *q.QueueParams
		groupID string
	}
	for _, cfg := range r.handlers {
		if cfg.queue.String() == queue.String() {
			toStop = append(toStop, struct {
				queue   *q.QueueParams
				groupID string
			}{cfg.queue, cfg.groupID})
		}
	}
	r.mu.RUnlock()

	for _, s := range toStop {
		r.StopHandler(s.queue, s.groupID)
	}
}

func (r *MessageHandlerRunner) onQueueLocked(queue *q.QueueParams) {
	r.log.Warn("queue locked — stopping handlers", "queue", queue.String())
	r.mu.RLock()
	var toStop []struct {
		queue   *q.QueueParams
		groupID string
	}
	for _, cfg := range r.handlers {
		if cfg.queue.String() == queue.String() {
			toStop = append(toStop, struct {
				queue   *q.QueueParams
				groupID string
			}{cfg.queue, cfg.groupID})
		}
	}
	r.mu.RUnlock()

	for _, s := range toStop {
		r.StopHandler(s.queue, s.groupID)
	}
}

func (r *MessageHandlerRunner) onQueueActive(queue *q.QueueParams) {
	r.log.Info("queue active — starting handlers", "queue", queue.String())
	r.mu.RLock()
	var toStart []struct {
		queue   *q.QueueParams
		groupID string
	}
	for _, cfg := range r.handlers {
		if cfg.queue.String() == queue.String() {
			toStart = append(toStart, struct {
				queue   *q.QueueParams
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

func (r *MessageHandlerRunner) reconcileLoop() {
	r.log.Debug("starting reconciliation loop")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			r.log.Debug("reconciliation loop stopped")
			return
		case <-ticker.C:
			r.reconcile()
		}
	}
}

func (r *MessageHandlerRunner) reconcile() {
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
		r.log.Debug("restarting handler", "queue", cfg.queue.String(), "group", cfg.groupID)
		mh := NewMessageHandler(r.consumerID, cfg.queue, cfg.groupID, cfg.handler, r.options)
		if err := mh.Run(r.ctx); err != nil {
			r.log.Error("failed to restart handler",
				"queue", cfg.queue.String(),
				"group", cfg.groupID,
				"error", err,
			)
			continue
		}
		r.mu.Lock()
		r.instances = append(r.instances, mh)
		r.mu.Unlock()
	}
}

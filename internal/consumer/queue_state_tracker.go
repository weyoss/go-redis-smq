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
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/weyoss/go-redis-smq/internal/eventbus"
	internalQueueEvents "github.com/weyoss/go-redis-smq/internal/queue/events"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type QueueStateTracker struct {
	mu      sync.RWMutex
	paused  map[string]*queue.QueueParams
	stopped map[string]*queue.QueueParams
	locked  map[string]*queue.QueueParams
	sub     *eventbus.Subscription

	onStopped func(queue *queue.QueueParams)
	onPaused  func(queue *queue.QueueParams)
	onLocked  func(queue *queue.QueueParams)
	onActive  func(queue *queue.QueueParams)

	log *slog.Logger
}

func NewQueueStateTracker(
	onStopped, onPaused, onLocked, onActive func(*queue.QueueParams),
) *QueueStateTracker {
	t := &QueueStateTracker{
		paused:    make(map[string]*queue.QueueParams),
		stopped:   make(map[string]*queue.QueueParams),
		locked:    make(map[string]*queue.QueueParams),
		onStopped: onStopped,
		onPaused:  onPaused,
		onLocked:  onLocked,
		onActive:  onActive,
		log:       logger.New("consumer", "queue-state-tracker"),
	}

	sub, err := internalQueueEvents.SubscribeStateChanged(func(p internalQueueEvents.StateChangedPayload) {
		t.handleStateChange(p)
	})
	if err != nil {
		t.log.Error("failed to subscribe to state change events", "error", err)
	}
	t.sub = sub

	t.log.Debug("queue state tracker started")

	return t
}

// decodeEventArg converts a positional event argument received from Redis
// Pub/Sub (typically a map[string]interface{}) into the target Go type.
func decodeEventArg(arg interface{}, target interface{}) error {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func (t *QueueStateTracker) handleStateChange(p internalQueueEvents.StateChangedPayload) {
	q := &p.Queue
	key := q.String()
	to := p.Transition.To
	from := "initial"
	if p.Transition.From != nil {
		from = p.Transition.From.String()
	}

	t.log.Info("queue state changed",
		"queue", key,
		"from", from,
		"to", to.String(),
		"reason", p.Transition.Reason,
	)

	t.mu.Lock()
	delete(t.stopped, key)
	delete(t.paused, key)
	delete(t.locked, key)

	switch to {
	case queue.StateStopped:
		t.stopped[key] = q
	case queue.StatePaused:
		t.paused[key] = q
	case queue.StateLocked:
		t.locked[key] = q
	}
	t.mu.Unlock()

	switch to {
	case queue.StateStopped:
		t.log.Debug("triggering onStopped callback", "queue", key)
		if t.onStopped != nil {
			t.onStopped(q)
		}
	case queue.StatePaused:
		t.log.Debug("triggering onPaused callback", "queue", key)
		if t.onPaused != nil {
			t.onPaused(q)
		}
	case queue.StateLocked:
		t.log.Debug("triggering onLocked callback", "queue", key)
		if t.onLocked != nil {
			t.onLocked(q)
		}
	case queue.StateActive:
		t.log.Debug("triggering onActive callback", "queue", key)
		if t.onActive != nil {
			t.onActive(q)
		}
	}
}

func (t *QueueStateTracker) IsStopped(queue *queue.QueueParams) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.stopped[queue.String()]
	return ok
}

func (t *QueueStateTracker) IsPaused(queue *queue.QueueParams) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.paused[queue.String()]
	return ok
}

func (t *QueueStateTracker) IsLocked(queue *queue.QueueParams) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.locked[queue.String()]
	return ok
}

func (t *QueueStateTracker) IsActive(queue *queue.QueueParams) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	key := queue.String()
	_, stopped := t.stopped[key]
	_, paused := t.paused[key]
	_, locked := t.locked[key]
	return !stopped && !paused && !locked
}

func (t *QueueStateTracker) Shutdown() {
	t.log.Debug("shutting down queue state tracker")

	if t.sub != nil {
		t.sub.Unsubscribe()
		t.log.Debug("unsubscribed from state change events")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	stoppedCount := len(t.stopped)
	pausedCount := len(t.paused)
	lockedCount := len(t.locked)

	t.paused = nil
	t.stopped = nil
	t.locked = nil

	t.log.Debug("queue state tracker shut down",
		"stoppedQueues", stoppedCount,
		"pausedQueues", pausedCount,
		"lockedQueues", lockedCount,
	)
}

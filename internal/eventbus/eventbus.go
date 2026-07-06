/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package eventbus

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
)

// EventBus is a Redis pub/sub event bus.
type EventBus struct {
	client   *redis.Client
	pubsub   *redis.PubSub
	handlers map[string][]handlerEntry

	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	started atomic.Bool
	closed  atomic.Bool
	log     *slog.Logger
}

// Handler is a callback for raw event handling.
// Prefer typed Subscribe* functions in domain event packages.
type Handler func(eventName string, payload json.RawMessage)

type handlerEntry struct {
	id      uint64
	handler Handler
}

var (
	instance *EventBus
	initMu   sync.Mutex
)

// Init initializes the singleton EventBus and starts listening.
func Init(ctx context.Context) {
	initMu.Lock()
	defer initMu.Unlock()

	if instance != nil {
		panic("eventbus: already initialized")
	}
	if ctx == nil {
		panic("eventbus: context must not be nil")
	}

	busCtx, cancel := context.WithCancel(ctx)
	instance = &EventBus{
		handlers: make(map[string][]handlerEntry),
		ctx:      busCtx,
		cancel:   cancel,
		client:   redisClient.Client(),
		log:      logger.New("eventbus"),
	}

	instance.started.Store(true)
	go instance.listen()

	instance.log.Info("eventbus initialized and listening")
}

// Singleton returns the initialized EventBus instance.
func Singleton() *EventBus {
	if instance == nil {
		panic("eventbus: Init() must be called before Singleton()")
	}
	return instance
}

// Shutdown gracefully stops the event bus.
func Shutdown() {
	initMu.Lock()
	defer initMu.Unlock()

	if instance != nil {
		instance.close()
		instance = nil
	}
}

func (eb *EventBus) close() {
	if !eb.closed.CompareAndSwap(false, true) {
		return
	}
	eb.cancel()

	eb.mu.Lock()
	if eb.pubsub != nil {
		eb.pubsub.Close()
		eb.pubsub = nil
	}
	eb.mu.Unlock()

	eb.started.Store(false)
	eb.log.Info("eventbus shutdown complete")
}

func (eb *EventBus) listen() {
	defer func() {
		if r := recover(); r != nil {
			eb.log.Error("listener panicked", "panic", r)
			eb.started.Store(false)
		}
	}()

	// Wait for first subscription
	for {
		eb.mu.RLock()
		hasHandlers := len(eb.handlers) > 0
		eb.mu.RUnlock()

		if hasHandlers {
			eb.mu.Lock()
			if eb.pubsub == nil && len(eb.handlers) > 0 {
				channels := make([]string, 0, len(eb.handlers))
				for ch := range eb.handlers {
					channels = append(channels, ch)
				}
				eb.pubsub = eb.client.Subscribe(eb.ctx, channels...)
				eb.log.Debug("subscribed to channels", "count", len(channels))
			}
			eb.mu.Unlock()
			break
		}

		select {
		case <-eb.ctx.Done():
			return
		default:
		}
	}

	eb.mu.RLock()
	pubsub := eb.pubsub
	eb.mu.RUnlock()

	if pubsub == nil {
		return
	}

	ch := pubsub.Channel()

	for {
		select {
		case <-eb.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			eb.dispatch(msg)
		}
	}
}

func (eb *EventBus) dispatch(msg *redis.Message) {
	var event Event
	if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
		eb.log.Error("failed to unmarshal event", "error", err)
		return
	}

	eb.mu.RLock()
	entries := eb.handlers[msg.Channel]
	handlers := make([]Handler, len(entries))
	for i, entry := range entries {
		handlers[i] = entry.handler
	}
	eb.mu.RUnlock()

	for _, handler := range handlers {
		eb.safeCall(handler, event.Name, event.Payload)
	}
}

func (eb *EventBus) safeCall(handler Handler, eventName string, payload json.RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			eb.log.Error("handler panicked", "event", eventName, "panic", r)
		}
	}()
	handler(eventName, payload)
}

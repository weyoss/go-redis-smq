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
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
)

// Handler is the callback signature for event bus subscriptions.
// It receives the original event name (without the namespace prefix)
// and the positional arguments decoded from the JSON array payload.
type Handler func(eventName string, args []interface{})

// EventBus is a Redis Pub/Sub based event bus with a namespace.
//
// It matches the TypeScript EventBusRedis behaviour:
//   - Events are published as a JSON array of positional arguments.
//   - Subscribers register handlers locally and the bus manages Redis
//     subscriptions idempotently.
//   - The namespace determines the Redis channel prefix.
type EventBus struct {
	namespace string
	client    *redis.Client
	pubsub    *redis.PubSub
	handlers  map[string][]handlerEntry

	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	started atomic.Bool
	closed  atomic.Bool
	done    chan struct{}
	log     *slog.Logger
}

type handlerEntry struct {
	id      uint64
	handler Handler
}

var nextHandlerID atomic.Uint64

// NewEventBus creates a new event bus instance with the given namespace.
//
// The namespace is used to build the Redis channel prefix:
//
//	redis-smq:events:{namespace}:*
//
// The Redis client must already be initialized before calling NewEventBus.
func NewEventBus(namespace string) *EventBus {
	return &EventBus{
		namespace: namespace,
		client:    redisClient.Client(),
		handlers:  make(map[string][]handlerEntry),
		done:      make(chan struct{}),
		log:       logger.New("eventbus", namespace),
	}
}

// Start initializes the event bus and begins listening for Redis messages.
// It is safe to call multiple times; subsequent calls are no-ops.
func (eb *EventBus) Start(ctx context.Context) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.started.Load() || eb.closed.Load() {
		return
	}

	eb.ctx, eb.cancel = context.WithCancel(ctx)
	eb.started.Store(true)
	eb.done = make(chan struct{})

	go func() {
		defer close(eb.done)
		eb.listen()
	}()

	eb.log.Info("event bus started", "namespace", eb.namespace)
}

// IsRunning reports whether the event bus is currently running.
func (eb *EventBus) IsRunning() bool {
	return eb.started.Load() && !eb.closed.Load()
}

// Shutdown gracefully stops the event bus and releases Redis subscriptions.
// It is safe to call multiple times.
func (eb *EventBus) Shutdown() {
	eb.mu.Lock()

	if !eb.closed.CompareAndSwap(false, true) {
		eb.mu.Unlock()
		return
	}

	if eb.cancel != nil {
		eb.cancel()
		eb.cancel = nil
	}

	if eb.pubsub != nil {
		_ = eb.pubsub.Close()
		eb.pubsub = nil
	}

	done := eb.done
	eb.mu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}

	eb.started.Store(false)
	eb.log.Info("event bus shut down", "namespace", eb.namespace)
}

// Publish publishes an event to Redis.
//
// The supplied arguments are serialized as a JSON array, matching the
// TypeScript wire format.
func (eb *EventBus) Publish(ctx context.Context, eventName string, args ...interface{}) error {
	if eb.closed.Load() {
		return fmt.Errorf("eventbus: closed")
	}

	channel := eb.channelName(eventName)
	payload, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("eventbus: marshal event %s: %w", eventName, err)
	}

	if err := eb.client.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("eventbus: publish %s: %w", eventName, err)
	}

	return nil
}

// Subscribe registers a handler for one or more event names.
//
// It returns a Subscription that can be used to unsubscribe from all the
// specified events at once.
func (eb *EventBus) Subscribe(handler Handler, eventNames ...string) (*Subscription, error) {
	if eb.closed.Load() {
		return nil, fmt.Errorf("eventbus: closed")
	}
	if len(eventNames) == 0 {
		return nil, fmt.Errorf("eventbus: at least one event name required")
	}

	id := nextHandlerID.Add(1)
	entry := handlerEntry{id: id, handler: handler}

	channels := make([]string, len(eventNames))
	for i, name := range eventNames {
		channels[i] = eb.channelName(name)
	}

	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.closed.Load() {
		return nil, fmt.Errorf("eventbus: closed")
	}

	for _, ch := range channels {
		eb.handlers[ch] = append(eb.handlers[ch], entry)
	}

	if err := eb.ensureSubscribed(channels); err != nil {
		for _, ch := range channels {
			eb.removeHandlerLocked(id, ch)
		}
		return nil, err
	}

	return &Subscription{
		id:  id,
		bus: eb,
		removeFn: func() {
			eb.removeSubscription(id, channels)
		},
	}, nil
}

// channelName builds the full Redis channel name for an event.
func (eb *EventBus) channelName(eventName string) string {
	return fmt.Sprintf("redis-smq:events:%s:%s", eb.namespace, eventName)
}

// eventNameFromChannel extracts the event name from a Redis channel.
func (eb *EventBus) eventNameFromChannel(channel string) string {
	prefix := fmt.Sprintf("redis-smq:events:%s:", eb.namespace)
	if strings.HasPrefix(channel, prefix) {
		return strings.TrimPrefix(channel, prefix)
	}
	return channel
}

// listen reads messages from the Redis pubsub channel and dispatches them.
func (eb *EventBus) listen() {
	defer func() {
		if r := recover(); r != nil {
			eb.log.Error("event bus listener panicked", "panic", r)
			eb.started.Store(false)
		}
	}()

	for {
		if eb.closed.Load() {
			return
		}

		eb.mu.RLock()
		pubsub := eb.pubsub
		ctx := eb.ctx
		eb.mu.RUnlock()

		if pubsub == nil || ctx == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}

		ch := pubsub.Channel()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				eb.dispatch(msg)
			}
		}
	}
}

// dispatch parses an incoming Redis message and calls registered handlers.
func (eb *EventBus) dispatch(msg *redis.Message) {
	var args []interface{}
	if err := json.Unmarshal([]byte(msg.Payload), &args); err != nil {
		eb.log.Error("failed to unmarshal event payload",
			"channel", msg.Channel,
			"error", err,
		)
		return
	}

	eventName := eb.eventNameFromChannel(msg.Channel)

	eb.mu.RLock()
	entries := eb.handlers[msg.Channel]
	handlers := make([]Handler, len(entries))
	for i, entry := range entries {
		handlers[i] = entry.handler
	}
	eb.mu.RUnlock()

	for _, handler := range handlers {
		eb.safeCall(handler, eventName, args)
	}
}

// safeCall invokes a handler and recovers from panics.
func (eb *EventBus) safeCall(handler Handler, eventName string, args []interface{}) {
	defer func() {
		if r := recover(); r != nil {
			eb.log.Error("handler panicked",
				"event", eventName,
				"panic", r,
			)
		}
	}()
	handler(eventName, args)
}

// ensureSubscribed subscribes to the given channels if not already
// subscribed.
//
// The PubSub connection is kept open for the lifetime of the bus. It is
// not closed when handlers become empty, because doing so would require
// re-creating the connection later and could race with the listener.
func (eb *EventBus) ensureSubscribed(channels []string) error {
	if eb.pubsub == nil {
		pubsub := eb.client.Subscribe(eb.ctx, channels...)
		eb.pubsub = pubsub
		return nil
	}

	for _, ch := range channels {
		if err := eb.pubsub.Subscribe(eb.ctx, ch); err != nil {
			return fmt.Errorf("eventbus: subscribe channel %s: %w", ch, err)
		}
	}
	return nil
}

// removeSubscription removes a handler subscription. It does not close the
// PubSub connection; the connection remains open until Shutdown.
func (eb *EventBus) removeSubscription(id uint64, channels []string) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	for _, ch := range channels {
		eb.removeHandlerLocked(id, ch)
	}
}

// removeHandlerLocked removes a handler entry from a channel's list.
func (eb *EventBus) removeHandlerLocked(id uint64, channel string) {
	entries := eb.handlers[channel]
	for i, entry := range entries {
		if entry.id == id {
			eb.handlers[channel] = append(entries[:i], entries[i+1:]...)
			break
		}
	}
	if len(eb.handlers[channel]) == 0 {
		delete(eb.handlers, channel)
	}
}

// Subscription represents a registered event handler.
type Subscription struct {
	id       uint64
	bus      *EventBus
	removeFn func()
}

// Unsubscribe removes this subscription and stops receiving events.
func (s *Subscription) Unsubscribe() {
	if s.removeFn != nil {
		s.removeFn()
		s.removeFn = nil
	}
}

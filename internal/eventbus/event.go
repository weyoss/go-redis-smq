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
	"sync/atomic"
)

// Event is the wire format for Redis pub/sub messages.
type Event struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Subscription represents a registered event handler.
type Subscription struct {
	id       uint64
	bus      *EventBus
	removeFn func()
}

// Unsubscribe removes this subscription.
func (s *Subscription) Unsubscribe() {
	if s.removeFn != nil {
		s.removeFn()
		s.removeFn = nil
	}
}

var nextHandlerID atomic.Uint64

// Publish sends an event to Redis.
func (eb *EventBus) Publish(ctx context.Context, eventName string, payload interface{}) error {
	if eb.closed.Load() {
		return fmt.Errorf("eventbus: closed")
	}

	event := map[string]interface{}{
		"name": eventName,
	}
	if payload != nil {
		event["payload"] = payload
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", eventName, err)
	}

	return eb.client.Publish(ctx, channelName(eventName), data).Err()
}

// Subscribe registers a handler for the given event names.
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
		channels[i] = channelName(name)
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

	sub := &Subscription{
		id:  id,
		bus: eb,
		removeFn: func() {
			eb.removeSubscription(id, channels)
		},
	}

	return sub, nil
}

func (eb *EventBus) ensureSubscribed(channels []string) error {
	if eb.pubsub == nil {
		eb.pubsub = eb.client.Subscribe(eb.ctx, channels...)
		return nil
	}
	return eb.pubsub.Subscribe(eb.ctx, channels...)
}

func (eb *EventBus) removeSubscription(id uint64, channels []string) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	for _, ch := range channels {
		eb.removeHandlerLocked(id, ch)
	}

	if len(eb.handlers) == 0 && eb.pubsub != nil {
		eb.pubsub.Close()
		eb.pubsub = nil
	}
}

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
		if eb.pubsub != nil {
			eb.pubsub.Unsubscribe(eb.ctx, channel)
		}
	}
}

func channelName(eventName string) string {
	return "redis-smq:events:" + eventName
}

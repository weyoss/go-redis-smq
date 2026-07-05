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
	"encoding/json"
	"log/slog"
	"time"

	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
)

// HeartbeatPayload is the JSON payload stored with each Heartbeat.
type HeartbeatPayload struct {
	Timestamp     int64  `json:"timestamp"`
	ComponentID   string `json:"componentId"`
	ComponentType string `json:"componentType"`
}

// Heartbeat manages periodic Heartbeat updates using Redis key expiry.
// Uses SET with PX (millisecond expiry) so dead consumers are auto-detected
// by key absence
type Heartbeat struct {
	id       string
	ttl      time.Duration
	interval time.Duration
	ticker   *time.Ticker
	done     chan struct{}
	log      *slog.Logger
}

// HeartbeatConfig holds configuration for a Heartbeat.
type HeartbeatConfig struct {
	ID  string
	TTL time.Duration
}

// NewHeartbeat creates a new Heartbeat manager.
// TTL is the key expiry time. Interval is auto-calculated as TTL/3
func NewHeartbeat(cfg HeartbeatConfig) *Heartbeat {
	ttl := cfg.TTL
	if ttl < 3*time.Second {
		ttl = 3 * time.Second
	}

	return &Heartbeat{
		id:       cfg.ID,
		ttl:      ttl,
		interval: ttl / 3,
		done:     make(chan struct{}),
		log:      logger.New("consumer", "heartbeat", cfg.ID),
	}
}

// Start begins sending Heartbeat updates in a background goroutine.
func (h *Heartbeat) Start(ctx context.Context) {
	h.log.Info("starting heartbeat", "ttl", h.ttl, "interval", h.interval)
	h.ticker = time.NewTicker(h.interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				h.log.Debug("heartbeat stopped by context")
				return
			case <-h.done:
				h.log.Debug("heartbeat stopped by done channel")
				return
			case <-h.ticker.C:
				h.beat(ctx)
			}
		}
	}()
}

// Stop stops the Heartbeat and deletes the key from Redis.
func (h *Heartbeat) Stop() {
	h.log.Info("stopping heartbeat")

	if h.ticker != nil {
		h.ticker.Stop()
	}

	// Safe close — prevents panic on double Stop
	select {
	case <-h.done:
		// Already closed
	default:
		close(h.done)
	}

	key := keys.System{}.ConsumerHeartbeat(h.id)
	if err := redisClient.Client().Del(context.Background(), key).Err(); err != nil {
		h.log.Error("failed to delete heartbeat key", "error", err)
	} else {
		h.log.Debug("heartbeat key deleted", "key", key)
	}
}

// beat sends a single Heartbeat: SET key payload PX ttl.
//
//	redisClient.set(this.heartbeatKey, payloadStr, { expire: { mode: 'PX', value: this.heartbeatTTL } })
func (h *Heartbeat) beat(ctx context.Context) {
	payload := HeartbeatPayload{
		Timestamp:     time.Now().UnixMilli(),
		ComponentID:   h.id,
		ComponentType: "consumer",
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		h.log.Error("failed to marshal heartbeat payload", "error", err)
		return
	}

	key := keys.System{}.ConsumerHeartbeat(h.id)
	if err := redisClient.Client().Set(ctx, key, string(payloadJSON), h.ttl).Err(); err != nil {
		h.log.Error("failed to send heartbeat", "error", err)
	}
}

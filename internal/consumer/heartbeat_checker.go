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

	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	redisKeys "github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
)

type HeartbeatChecker struct {
	log *slog.Logger
}

func NewHeartbeatChecker() *HeartbeatChecker {
	return &HeartbeatChecker{
		log: logger.New("consumer", "heartbeat-checker"),
	}
}

func (hc *HeartbeatChecker) IsAlive(ctx context.Context, consumerID string) (bool, error) {
	key := redisKeys.System{}.ConsumerHeartbeat(consumerID)
	exists, err := redisClient.Client().Exists(ctx, key).Result()
	if err != nil {
		hc.log.Debug("failed to check heartbeat", "consumerID", consumerID, "error", err)
		return false, nil
	}
	return exists > 0, nil
}

func (hc *HeartbeatChecker) AreAlive(ctx context.Context, consumerIDs []string) (map[string]bool, error) {
	result := make(map[string]bool, len(consumerIDs))
	if len(consumerIDs) == 0 {
		return result, nil
	}

	heartbeatKeys := make([]string, len(consumerIDs))
	for i, cid := range consumerIDs {
		heartbeatKeys[i] = redisKeys.System{}.ConsumerHeartbeat(cid)
	}

	vals, err := redisClient.Client().MGet(ctx, heartbeatKeys...).Result()
	if err != nil {
		hc.log.Error("failed to check consumer heartbeats",
			"count", len(consumerIDs),
			"error", err,
		)
		return nil, fmt.Errorf("check consumers: %w", err)
	}

	aliveCount := 0
	deadCount := 0
	for i, cid := range consumerIDs {
		alive := vals[i] != nil
		result[cid] = alive
		if alive {
			aliveCount++
		} else {
			deadCount++
		}
	}

	hc.log.Debug("heartbeat check completed",
		"total", len(consumerIDs),
		"alive", aliveCount,
		"dead", deadCount,
	)

	return result, nil
}

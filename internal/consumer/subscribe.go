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
	"fmt"
	"os"
	"time"

	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type consumerInfo struct {
	IPAddresses []string `json:"ipAddress"`
	Hostname    string   `json:"hostname"`
	PID         int      `json:"pid"`
	CreatedAt   int64    `json:"createdAt"`
}

func SubscribeConsumer(ctx context.Context, consumerID string, q *queue.QueueParams, groupID string) error {
	log := logger.New("consumer", "subscribe", consumerID, q.Name())

	info := consumerInfo{
		IPAddresses: localIPs(),
		Hostname:    hostname(),
		PID:         os.Getpid(),
		CreatedAt:   time.Now().UnixMilli(),
	}

	log.Debug("subscribing consumer",
		"hostname", info.Hostname,
		"pid", info.PID,
		"group", groupID,
	)

	qKey := keys.Queue{Namespace: q.NS(), Name: q.Name()}

	luaKeys := []string{
		qKey.Properties(),
		qKey.Consumers(),
		keys.System{}.ConsumerQueues(consumerID),
		qKey.ProcessingQueues(),
		qKey.ConsumerProcessing(consumerID),
	}

	if groupID != "" {
		luaKeys = append(luaKeys, qKey.ConsumerGroupMembers(groupID))
	}

	consumerInfoJSON, _ := json.Marshal(info)
	queueJSON, _ := json.Marshal(q)

	argv := []interface{}{
		consumerID,
		string(consumerInfoJSON),
		string(queueJSON),
		"11",
		queue.StateActive.Int(),
	}

	reply, err := redisClient.Eval(ctx, scripts.SubscribeConsumer, luaKeys, argv...)
	if err != nil {
		log.Error("subscribe script failed", "error", err)
		return fmt.Errorf("subscribe consumer: %w", err)
	}

	replyStr, err := redisClient.String(reply)
	if err != nil {
		log.Error("failed to parse subscribe reply", "error", err)
		return err
	}

	switch replyStr {
	case "OK":
		log.Info("consumer subscribed successfully")
		return nil
	case "QUEUE_NOT_FOUND":
		log.Warn("queue not found")
		return fmt.Errorf("queue not found: %s/%s", q.NS(), q.Name())
	case "QUEUE_NOT_ACTIVE":
		log.Warn("queue not active")
		return fmt.Errorf("queue not active: %s/%s", q.NS(), q.Name())
	default:
		log.Error("unexpected subscribe reply", "reply", replyStr)
		return fmt.Errorf("unexpected script reply: %s", replyStr)
	}
}

func UnsubscribeConsumer(ctx context.Context, consumerID string, queue *queue.QueueParams, groupID string) error {
	log := logger.New("consumer", "unsubscribe", consumerID, queue.Name())

	log.Debug("unsubscribing consumer", "group", groupID)

	qKey := keys.Queue{Namespace: queue.NS(), Name: queue.Name()}

	luaKeys := []string{
		qKey.Properties(),
		qKey.Consumers(),
		keys.System{}.ConsumerQueues(consumerID),
		qKey.ProcessingQueues(),
		qKey.ConsumerProcessing(consumerID),
	}

	if groupID != "" {
		luaKeys = append(luaKeys, qKey.ConsumerGroupMembers(groupID))
	}

	queueJSON, _ := json.Marshal(queue)

	argv := []interface{}{
		consumerID,
		string(queueJSON),
	}

	reply, err := redisClient.Eval(ctx, scripts.UnsubscribeConsumer, luaKeys, argv...)
	if err != nil {
		log.Error("unsubscribe script failed", "error", err)
		return fmt.Errorf("unsubscribe consumer: %w", err)
	}

	replyStr, err := redisClient.String(reply)
	if err != nil {
		log.Error("failed to parse unsubscribe reply", "error", err)
		return err
	}

	switch replyStr {
	case "OK":
		log.Info("consumer unsubscribed successfully")
		return nil
	case "QUEUE_NOT_FOUND":
		log.Warn("queue not found during unsubscribe")
		return fmt.Errorf("queue not found: %s/%s", queue.NS(), queue.Name())
	case "PROCESSING_QUEUE_NOT_EMPTY":
		log.Warn("processing queue not empty during unsubscribe",
			"consumerID", consumerID,
		)
		return fmt.Errorf("processing queue not empty for consumer %s on queue %s/%s", consumerID, queue.NS(), queue.Name())
	default:
		log.Error("unexpected unsubscribe reply", "reply", replyStr)
		return fmt.Errorf("unexpected script reply: %s", replyStr)
	}
}

func hostname() string {
	name, _ := os.Hostname()
	return name
}

func localIPs() []string {
	return []string{"127.0.0.1"}
}

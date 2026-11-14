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
	"net"
	"os"
	"time"

	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// SubscribeConsumer registers a consumer with a queue.
func SubscribeConsumer(ctx context.Context, consumerID string, q *queue.Params, groupID string) error {
	log := logger.New("consumer", "subscribe", consumerID, q.Name())

	info := queue.QueueConsumer{
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
		qSchema.OperationalState.Key(),
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

func UnsubscribeConsumer(ctx context.Context, consumerID string, queue *queue.Params, groupID string) error {
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

// localIPs returns every non-internal IPv4 address of the host, as
// reported by net.InterfaceAddrs() at the time of the call.
//
// Matches TypeScript's behavior when building IQueueConsumer.ipAddress:
//
//   - IPv4 only. IPv6 addresses (including ::1) are excluded
//
//   - Loopback (127.0.0.0/8) is excluded. Loopback addresses are
//     not remotely reachable and give an operator no diagnostic signal.
//
//   - Link-local IPv4 (169.254.0.0/16) is included. This is a corner case
//     (interface without DHCP lease) that some operators do want to see
//     reflected in the consumer registry.
//
// Returns an empty (non-nil) slice if no non-internal IPv4 address
// exists — an unusual state, but possible on a container with only
// loopback connectivity, or a host whose only interface is down.
//
// Errors from InterfaceAddrs are swallowed and treated as "no addresses".
// IP discovery is best-effort diagnostic metadata, not a critical
// operational input. A failure here (or any transient issue reading the
// interface table) must not prevent consumer subscription — the
// subscription write succeeds with an empty ipAddress list.
func localIPs() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return []string{}
	}

	ips := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			// net.InterfaceAddrs returns *net.IPNet on every platform
			// Go targets, but the type assertion is defensive — a
			// future platform addition (or a test stub) that returns
			// something else should be skipped, not panic.
			continue
		}
		ip := ipNet.IP
		if ip.To4() == nil {
			// IPv6 — excluded.
			continue
		}
		if ip.IsLoopback() {
			// 127.0.0.0/8 — excluded.
			continue
		}
		ips = append(ips, ip.String())
	}
	return ips
}

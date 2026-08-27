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

	internalQueue "github.com/weyoss/go-redis-smq/internal/queue"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// PrepareConsumerGroup ensures a consumer group exists for PUB/SUB queues.
// If no groupID is provided, creates an ephemeral group based on the consumer ID.
// Returns the effective group ID to use.
//
//   - Load queue properties
//   - If PUB/SUB: create consumer group (ephemeral if no groupID provided)
//   - If POINT_TO_POINT with groupID: error
//   - If POINT_TO_POINT without groupID: OK, no group needed
func PrepareConsumerGroup(ctx context.Context, consumerID string, q *queue.Params, groupID string) (string, error) {
	log := logger.New("consumer", "prepare-group", consumerID, q.Name())

	store := internalQueue.NewManager().Store()
	props, err := store.Load(ctx, q)
	if err != nil {
		log.Error("failed to load queue properties", "error", err)
		return "", fmt.Errorf("prepare consumer group: %w", err)
	}

	log.Debug("loaded queue properties",
		"deliveryModel", props.DeliveryModel.String(),
	)

	// PUB/SUB queues require a consumer group
	if props.DeliveryModel == queue.DeliveryPubSub {
		effectiveGroupID := groupID
		if effectiveGroupID == "" {
			effectiveGroupID = ephemeralGroupID(consumerID)
			log.Debug("generated ephemeral group ID", "group", effectiveGroupID)
		}

		if err := createConsumerGroup(ctx, q, effectiveGroupID); err != nil {
			log.Error("failed to create consumer group", "group", effectiveGroupID, "error", err)
			return "", fmt.Errorf("prepare consumer group: %w", err)
		}

		log.Debug("consumer group prepared", "group", effectiveGroupID)
		return effectiveGroupID, nil
	}

	// POINT_TO_POINT with groupID is not supported
	if groupID != "" {
		log.Warn("consumer group not supported for point-to-point queue", "group", groupID)
		return "", fmt.Errorf("consumer group not supported for point-to-point queues")
	}

	// POINT_TO_POINT without groupID — no group needed
	log.Debug("point-to-point queue — no consumer group needed")
	return "", nil
}

// DeleteEphemeralConsumerGroup removes an ephemeral consumer group.
// Called during consumer shutdown. If groupID is empty, generates one from consumerID.
func DeleteEphemeralConsumerGroup(ctx context.Context, consumerID string, q *queue.Params, groupID string) error {
	log := logger.New("consumer", "delete-group", consumerID, q.Name())

	effectiveGroupID := groupID
	if effectiveGroupID == "" {
		effectiveGroupID = ephemeralGroupID(consumerID)
	}

	log.Debug("deleting ephemeral consumer group", "group", effectiveGroupID)

	qKey := keys.Queue{
		Namespace: q.NS(),
		Name:      q.Name(),
	}

	luaKeys := []string{
		qKey.ConsumerGroups(),
		qKey.Pending(),
		qKey.Priority(),
		qKey.Properties(),
		qKey.ConsumerGroupMembers(effectiveGroupID),
	}

	argv := []interface{}{
		qSchema.QueueFieldType.Key(),
		queue.TypePriority.Int(),
		qSchema.QueueFieldDeliveryModel.Key(),
		queue.DeliveryPubSub.Int(),
		effectiveGroupID,
		qSchema.QueueFieldOperationalState.Key(),
		queue.StateLocked.String(),
		qSchema.QueueFieldLockID.Key(),
		"",
	}

	reply, err := redis.Eval(ctx, scripts.DeleteConsumerGroup, luaKeys, argv...)
	if err != nil {
		log.Error("failed to delete consumer group", "group", effectiveGroupID, "error", err)
		return fmt.Errorf("delete ephemeral consumer group: %w", err)
	}

	if fmt.Sprintf("%v", reply) != "OK" {
		log.Warn("unexpected reply when deleting consumer group",
			"group", effectiveGroupID,
			"reply", fmt.Sprintf("%v", reply),
		)
		return fmt.Errorf("delete ephemeral consumer group: unexpected reply: %v", reply)
	}

	log.Debug("ephemeral consumer group deleted", "group", effectiveGroupID)
	return nil
}

// ephemeralGroupID generates a unique consumer group ID for ephemeral groups.
func ephemeralGroupID(consumerID string) string {
	return "cid-" + consumerID
}

// createConsumerGroup creates a consumer group for a queue.
func createConsumerGroup(ctx context.Context, q *queue.Params, groupID string) error {
	log := logger.New("consumer", "create-group", q.Name())

	qKey := keys.Queue{
		Namespace: q.NS(),
		Name:      q.Name(),
	}

	result, err := redis.Client().SAdd(ctx, qKey.ConsumerGroups(), groupID).Result()
	if err != nil {
		log.Error("failed to create consumer group", "group", groupID, "error", err)
		return fmt.Errorf("create consumer group: %w", err)
	}

	if result == 1 {
		log.Debug("consumer group created", "group", groupID)
	} else {
		log.Debug("consumer group already exists", "group", groupID)
	}

	return nil
}

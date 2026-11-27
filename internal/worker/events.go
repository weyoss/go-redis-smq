/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package worker

import (
	"context"

	"github.com/weyoss/go-redis-smq/internal/eventmultiplexer"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scheduler event names. Published by scheduledPublisher for events that
// happen at fire time (not at produce time — those are producer events).
const (
	EventMessagePublished    = "scheduler.messagePublished"
	EventMessageDeadLettered = "scheduler.messageDeadLettered"
)

var schedulerLog = logger.New("scheduler", "events")

func publishSchedulerEvent(ctx context.Context, eventName string, args ...interface{}) {
	if err := eventmultiplexer.Publish(ctx, eventName, args...); err != nil {
		schedulerLog.Error("failed to publish event",
			"event", eventName,
			"error", err,
		)
	}
}

// PublishMessagePublished fires scheduler.messagePublished — a scheduled
// message has been moved into a pending list by the scheduled publisher.
//
// Not to be confused with internal/producer's PublishMessagePublished,
// which fires producer.messagePublished for non-scheduled produces. Same
// payload shape minus producerID — the scheduler is not a producer.
func PublishMessagePublished(ctx context.Context, messageID string, queue queue.ParsedParams) {
	publishSchedulerEvent(ctx, EventMessagePublished, messageID, queue)
}

// PublishMessageDeadLettered fires scheduler.messageDeadLettered — a
// scheduled message could not be delivered because its target consumer
// group no longer existed at fire time. The message is now in the
// dead-letter list instead of a pending list.
func PublishMessageDeadLettered(ctx context.Context, messageID string, queue queue.ParsedParams) {
	publishSchedulerEvent(ctx, EventMessageDeadLettered, messageID, queue)
}

/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package scheduler provides the public API for observing scheduled-message
// events that fire at delivery time.
//
// # Event Taxonomy
//
// Scheduled messages go through a different lifecycle than immediately
// published ones. The full set of public events is:
//
//   - producer.messagePublished — a non-scheduled produce succeeded.
//   - producer.messageScheduled — a scheduled produce succeeded; the
//     message is now in the scheduled set (see pkg/producer).
//   - scheduler.messagePublished — a scheduled message was due and has
//     been moved into a pending list, ready for consumption.
//   - scheduler.messageDeadLettered — a scheduled message was due but
//     could not be delivered because its target consumer group no longer
//     exists. The message is now in the dead-letter list instead.
//
// The producer-pair (messagePublished/messageScheduled) fires at produce
// time; this package's events fire later, whenever the message becomes
// due. Subscribers that need both "was produced" and "was delivered"
// signals subscribe to both packages.
//
// # Example
//
//	sub, err := scheduler.SubscribeMessagePublished(func(p scheduler.MessagePublishedPayload) {
//	    fmt.Printf("Due: %s on %s\n", p.MessageID, p.Queue.QueueParams.Name())
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer sub.Unsubscribe()
//
// Like all public subscriptions, the user event bus must be started with
// redissmq.InitUserEventBus(ctx) before subscribing.
package scheduler

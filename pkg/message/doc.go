/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package message provides the public API for managing RedisSMQ messages.
//
// It contains the data types and interfaces needed to create, inspect,
// delete, and requeue messages. The package itself does not implement
// storage or message lifecycle logic; those are provided by the root
// redissmq package through the factory function redissmq.NewMessageManager().
//
// # Concrete Implementation
//
// The Manager interface defines the operations available on messages. Use
// redissmq.NewMessageManager() to obtain a concrete instance:
//
//	mm := redissmq.NewMessageManager()
//	msg, err := mm.Get(ctx, messageID)
//
// # Producible Messages
//
// Use ProducibleMessage (via message.New()) to configure a message before
// publishing:
//
//	m := message.New().
//	    SetBody(map[string]interface{}{"userId": 123}).
//	    SetQueue(queueParams).
//	    SetTTL(5 * time.Minute).
//	    SetPriority(message.PriorityHigh)
//
// # Transferable Messages
//
// When a message is delivered to a consumer, it is represented as a
// Transferable. This type carries the full message payload, metadata, and
// lifecycle state. Handlers receive a *Transferable and return nil to
// acknowledge the message or an error to trigger retries/dead-lettering.
//
// # Message Status and State
//
// Status describes the current lifecycle stage (pending, processing,
// acknowledged, dead-lettered, etc.), while StateTransferable holds detailed
// counters and timestamps such as attempts, expiry, and scheduling
// information.
//
// # Requeueing
//
// Acknowledged or dead-lettered messages can be requeued using the Manager's
// Requeue method. This creates a new message based on the original while
// preserving relevant metadata.
//
// # Example
//
//	mm := redissmq.NewMessageManager()
//	m, err := mm.Get(ctx, "msg-123")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Status:", m.Status)
package message

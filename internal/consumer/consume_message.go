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
	"sync/atomic"
	"time"

	consumerEvents "github.com/weyoss/go-redis-smq/internal/consumer/events"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// ConsumeMessage handles a single dequeued message. It invokes the user
// handler, enforces expiration and consume timeouts, and routes the result
// to the batch acker or unacker.
type ConsumeMessage struct {
	queue        *q.QueueParams
	groupID      string
	consumerID   string
	handler      Handler
	batchAcker   *BatchAcker
	batchUnacker *BatchUnacker
	log          *slog.Logger
}

// NewConsumeMessage creates a new ConsumeMessage instance.
func NewConsumeMessage(
	queue *q.QueueParams,
	groupID string,
	consumerID string,
	handler Handler,
	batchAcker *BatchAcker,
	batchUnacker *BatchUnacker,
) *ConsumeMessage {
	return &ConsumeMessage{
		queue:        queue,
		groupID:      groupID,
		consumerID:   consumerID,
		handler:      handler,
		batchAcker:   batchAcker,
		batchUnacker: batchUnacker,
		log:          logger.New("consumer", "consume", consumerID, queue.Name()),
	}
}

// Consume processes a dequeued message envelope.
func (c *ConsumeMessage) Consume(ctx context.Context, envelope *internalMessage.Envelope) {
	m := envelope.ToTransferable()

	// Publish message received event.
	consumerEvents.PublishMessageReceived(ctx, m.ID, *c.queue, c.consumerID)

	c.log.Debug("consuming message", "messageID", m.ID)

	// Handle expired messages immediately.
	if c.isExpired(m) {
		c.log.Warn("message expired — unacknowledging", "messageID", m.ID, "ttl", m.TTL)
		c.batchUnacker.Unack(envelope, CauseTTLExpired)
		return
	}

	// Apply consume timeout if set. The handler receives a deadline context
	// as a hint, but the forced timer below is authoritative.
	ctx, cancel := c.handlerContext(ctx, m)
	defer cancel()

	// Forced timeout: unacknowledge the message if the handler doesn't
	// finish in time.
	var timedOut atomic.Bool
	if m.ConsumeTimeout > 0 {
		go func() {
			<-ctx.Done()
			if ctx.Err() == context.DeadlineExceeded && timedOut.CompareAndSwap(false, true) {
				c.log.Warn("handler timed out — unacknowledging", "messageID", m.ID)
				c.batchUnacker.Unack(envelope, CauseTimeout)
			}
		}()
	}

	// Invoke handler with panic recovery.
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				c.log.Error("handler panicked", "messageID", m.ID, "panic", r)
				err = fmt.Errorf("handler panic: %v", r)
			}
		}()
		err = c.invokeHandler(ctx, m)
	}()

	// If the forced timer already fired, the message was already
	// unacknowledged. Do not ack or unack again.
	if timedOut.Load() {
		return
	}

	// Normal ack/unack path.
	if err != nil {
		c.log.Warn("handler returned error — unacknowledging", "messageID", m.ID, "error", err)
		c.batchUnacker.Unack(envelope, CauseUnacknowledged)
	} else {
		c.log.Debug("handler succeeded — acknowledging", "messageID", m.ID)
		c.batchAcker.Ack(m.ID)
		consumerEvents.PublishMessageAcknowledged(ctx, m.ID, *c.queue, c.consumerID)
	}
}

// isExpired checks whether a message has exceeded its TTL.
func (c *ConsumeMessage) isExpired(m *msg.Transferable) bool {
	if m.TTL <= 0 {
		return false
	}
	return time.Since(time.UnixMilli(m.CreatedAt)) >= time.Duration(m.TTL)*time.Millisecond
}

// handlerContext returns a context with a timeout if the message has a
// consume timeout configured.
func (c *ConsumeMessage) handlerContext(ctx context.Context, m *msg.Transferable) (context.Context, context.CancelFunc) {
	if m.ConsumeTimeout > 0 {
		timeout := time.Duration(m.ConsumeTimeout) * time.Millisecond
		c.log.Debug("applying consume timeout",
			"messageID", m.ID,
			"timeout", timeout,
		)
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

// invokeHandler calls the user-supplied handler.
func (c *ConsumeMessage) invokeHandler(ctx context.Context, m *msg.Transferable) error {
	return c.handler(ctx, m)
}

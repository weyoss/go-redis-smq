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
	"errors"
	"fmt"
	"log/slog"
	"time"

	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	mSchema "github.com/weyoss/go-redis-smq/internal/message/schema"
	internalQueue "github.com/weyoss/go-redis-smq/internal/queue"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type DequeueMessage struct {
	queue      *queue.QueueParams
	groupID    string
	consumerID string
	blocking   bool

	queueType      *queue.QueueType
	rateLimit      *queue.RateLimitParams
	unacknowledger *MessageUnacknowledger
	log            *slog.Logger
}

type DequeueOption func(*DequeueMessage)

func WithBlocking() DequeueOption {
	return func(d *DequeueMessage) {
		d.blocking = true
	}
}

func NewDequeueMessage(queue *queue.QueueParams, groupID, consumerID string, opts ...DequeueOption) *DequeueMessage {
	d := &DequeueMessage{
		queue:      queue,
		groupID:    groupID,
		consumerID: consumerID,
		blocking:   false,
		log:        logger.New("consumer", "dequeue", consumerID, queue.Name()),
	}
	for _, o := range opts {
		o(d)
	}
	d.unacknowledger = NewMessageUnacknowledger(queue, consumerID)
	return d
}

func (d *DequeueMessage) Init(ctx context.Context) error {
	store := internalQueue.NewManager().Store()
	props, err := store.Load(ctx, d.queue)
	if err != nil {
		d.log.Error("failed to load queue properties", "error", err)
		return fmt.Errorf("dequeue init: %w", err)
	}
	d.queueType = &props.Type
	d.rateLimit = props.RateLimit

	d.log.Info("dequeuer initialized",
		"queueType", props.Type.String(),
		"hasRateLimit", props.RateLimit != nil,
	)
	if props.RateLimit != nil {
		d.log.Debug("rate limit configured",
			"limit", props.RateLimit.Limit(),
			"interval", props.RateLimit.Interval(),
		)
	}

	return nil
}

func (d *DequeueMessage) Dequeue(ctx context.Context) (*internalMessage.Envelope, error) {
	if d.queueType == nil {
		return nil, fmt.Errorf("dequeue: not initialized, call Init first")
	}

	if d.rateLimit != nil {
		ok, err := d.checkRateLimit(ctx)
		if err != nil {
			d.log.Error("rate limit check failed", "error", err)
			return nil, fmt.Errorf("dequeue: rate limit check: %w", err)
		}
		if !ok {
			d.log.Debug("rate limit exceeded — skipping dequeue")
			return nil, nil
		}
	}

	messageID, err := d.pop(ctx)
	if err != nil {
		d.log.Error("pop failed", "error", err)
		return nil, fmt.Errorf("dequeue: pop: %w", err)
	}
	if messageID == "" {
		return nil, nil
	}

	d.log.Debug("popped message", "messageID", messageID)

	env, err := d.checkout(ctx, messageID)
	if err != nil {
		// Checkout failed due to a queue state error. The message was already
		// moved into the processing queue by pop(). We must unacknowledge it
		// immediately with the exact cause so it isn't left stranded.
		d.unacknowledgePoppedMessage(ctx, messageID, err)
		return nil, err
	}
	if env == nil {
		// Message was not pending or not found – already removed by checkout script.
		return nil, nil
	}

	return env, nil
}

func (d *DequeueMessage) unacknowledgePoppedMessage(ctx context.Context, messageID string, checkoutErr error) {
	cause := d.causeFromCheckoutError(checkoutErr)
	if cause == nil {
		// No specific cause (e.g., MESSAGE_NOT_FOUND) – message was already handled by the script.
		return
	}

	// Build a minimal envelope so the unacknowledger can construct the Lua arguments.
	msg := msg.New().SetBody("").SetQueue(d.queue)
	msg.SetConsumeTimeout(0)
	envelope := internalMessage.NewEnvelope(msg)
	envelope.MessageState().SetID(messageID)
	envelope.SetDestinationQueue(d.queue)
	if d.groupID != "" {
		envelope.SetConsumerGroupID(d.groupID)
	}

	entries := []UnackEntry{{Message: envelope, Cause: *cause}}
	if err := d.unacknowledger.UnacknowledgeBatch(ctx, entries); err != nil {
		d.log.Error("failed to unacknowledge popped message after checkout error",
			"messageID", messageID, "cause", int(*cause), "checkoutErr", checkoutErr, "unackErr", err)
	}
}

func (d *DequeueMessage) causeFromCheckoutError(err error) *UnacknowledgeCause {
	if errors.Is(err, consumer.ErrQueueStopped) {
		c := CauseQueueStopped
		return &c
	}
	if errors.Is(err, consumer.ErrQueueLocked) {
		c := CauseQueueLocked
		return &c
	}
	if errors.Is(err, consumer.ErrQueueInvalidState) {
		c := CauseQueueInvalidState
		return &c
	}
	return nil
}

func (d *DequeueMessage) pop(ctx context.Context) (string, error) {
	qKey := keys.Queue{Namespace: d.queue.NS(), Name: d.queue.Name()}
	dst := qKey.ConsumerProcessing(d.consumerID)

	if *d.queueType == queue.TypePriority {
		return d.popPriority(ctx, qKey.Priority(), dst)
	}
	return d.popFIFO(ctx, qKey.Pending(), dst)
}

func (d *DequeueMessage) popPriority(ctx context.Context, src, dst string) (string, error) {
	reply, err := redisClient.Eval(ctx, scripts.ZPOPLPUSH, []string{src, dst})
	if err != nil || reply == nil {
		return "", nil
	}
	return fmt.Sprintf("%v", reply), nil
}

func (d *DequeueMessage) popFIFO(ctx context.Context, src, dst string) (string, error) {
	if d.blocking && d.rateLimit == nil {
		val, err := redisClient.Client().BRPopLPush(ctx, src, dst, 0).Result()
		if err != nil {
			return "", nil
		}
		return val, nil
	}
	val, err := redisClient.Client().RPopLPush(ctx, src, dst).Result()
	if err != nil {
		return "", nil
	}
	return val, nil
}

func (d *DequeueMessage) checkRateLimit(ctx context.Context) (bool, error) {
	if d.rateLimit == nil {
		return true, nil
	}

	qKey := keys.Queue{Namespace: d.queue.NS(), Name: d.queue.Name()}

	reply, err := redisClient.Eval(ctx, scripts.CheckRateLimit,
		[]string{qKey.RateLimit()},
		[]interface{}{
			d.rateLimit.Limit(),
			d.rateLimit.Interval().Milliseconds(),
		},
	)
	if err != nil {
		return false, err
	}

	n, err := redisClient.Int64(reply)
	if err != nil {
		return false, err
	}
	return n == 0, nil
}

func (d *DequeueMessage) checkout(ctx context.Context, messageID string) (*internalMessage.Envelope, error) {
	msgKey := keys.System{}.Message(messageID)
	qKey := keys.Queue{Namespace: d.queue.NS(), Name: d.queue.Name()}

	reply, err := redisClient.Eval(ctx, scripts.CheckoutMessage,
		[]string{msgKey, qKey.Properties()},
		[]interface{}{
			mSchema.MessageFieldProcessingStartedAt.Key(),
			mSchema.MessageFieldLastProcessedAt.Key(),
			time.Now().UnixMilli(),
			mSchema.MessageFieldStatus.Key(),
			msg.StatusProcessing.Int(),
			msg.StatusPending.Int(),
			mSchema.MessageFieldAttempts.Key(),
			qSchema.QueueFieldProcessingMessagesCount.Key(),
			qSchema.QueueFieldPendingMessagesCount.Key(),
			qSchema.QueueFieldOperationalState.Key(),
			queue.StateActive.Int(),
			queue.StatePaused.Int(),
			queue.StateStopped.Int(),
			queue.StateLocked.Int(),
		},
	)
	if err != nil {
		d.log.Error("checkout script failed", "messageID", messageID, "error", err)
		return nil, fmt.Errorf("checkout script: %w", err)
	}

	if replyStr, ok := reply.(string); ok {
		switch replyStr {
		case "QUEUE_STOPPED":
			d.log.Warn("queue stopped — cannot checkout", "messageID", messageID)
			return nil, consumer.ErrQueueStopped
		case "QUEUE_LOCKED":
			d.log.Warn("queue locked — cannot checkout", "messageID", messageID)
			return nil, consumer.ErrQueueLocked
		case "QUEUE_INVALID_STATE":
			d.log.Warn("queue in invalid state — cannot checkout", "messageID", messageID)
			return nil, consumer.ErrQueueInvalidState
		case "MESSAGE_NOT_FOUND":
			d.log.Debug("message not found in queue", "messageID", messageID)
			return nil, nil
		case "MESSAGE_NOT_PENDING":
			d.log.Debug("message no longer pending", "messageID", messageID)
			return nil, nil
		default:
			d.log.Error("unexpected script reply", "messageID", messageID, "reply", replyStr)
			return nil, fmt.Errorf("unexpected script reply: %s", replyStr)
		}
	}

	if reply == nil {
		return nil, nil
	}

	hash, ok := reply.([]interface{})
	if !ok {
		d.log.Error("invalid script reply type", "messageID", messageID, "type", fmt.Sprintf("%T", reply))
		return nil, fmt.Errorf("invalid script reply type: %T", reply)
	}

	hashMap := make(map[string]string, len(hash)/2)
	for i := 0; i < len(hash); i += 2 {
		if i+1 < len(hash) {
			hashMap[fmt.Sprintf("%v", hash[i])] = fmt.Sprintf("%v", hash[i+1])
		}
	}

	codec := internalMessage.NewEnvelopeCodec()
	envelope, err := codec.DecodeHash(ctx, hashMap)
	if err != nil {
		d.log.Error("failed to decode message", "messageID", messageID, "error", err)
		return nil, fmt.Errorf("decode message: %w", err)
	}

	d.log.Debug("message checked out", "messageID", messageID, "attempts", envelope.MessageState().Attempts())

	return envelope, nil
}

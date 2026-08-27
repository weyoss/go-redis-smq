/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	internalexchange "github.com/weyoss/go-redis-smq/internal/exchange"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	publicexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	publicproducer "github.com/weyoss/go-redis-smq/pkg/producer"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Producer is the concrete implementation of the public producer interface.
type Producer struct {
	mu             sync.RWMutex
	running        bool
	id             string
	directExchange *internalexchange.DirectStore
	fanoutExchange *internalexchange.FanoutStore
	topicExchange  *internalexchange.TopicStore
	pubSubResolver *PubSubTargetResolver
	log            *slog.Logger
}

// New creates a new producer that satisfies the public producer interface.
func New() publicproducer.Producer {
	id := uuid.New().String()
	em := internalexchange.NewManager()
	return &Producer{
		id:             id,
		directExchange: em.Direct(),
		fanoutExchange: em.Fanout(),
		topicExchange:  em.Topic(),
		log:            logger.New("producer", "manager", id),
	}
}

// Run starts the producer and prepares it for publishing.
func (prod *Producer) Run(ctx context.Context) error {
	prod.mu.Lock()
	defer prod.mu.Unlock()

	if prod.running {
		prod.log.Debug("already running")
		return nil
	}

	prod.log.Info("starting producer")

	PublishGoingUp(ctx, prod.id)

	prod.pubSubResolver = NewPubSubTargetResolver(prod.id)
	if err := prod.pubSubResolver.Load(ctx); err != nil {
		prod.pubSubResolver = nil
		prod.log.Error("failed to load pub/sub targets", "error", err)
		return fmt.Errorf("producer: load pub/sub targets: %w", err)
	}

	prod.running = true

	go func() {
		<-ctx.Done()
		prod.Shutdown(context.Background())
	}()

	prod.log.Info("producer started")

	PublishUp(ctx, prod.id)

	return nil
}

// Shutdown gracefully stops the producer.
func (prod *Producer) Shutdown(ctx context.Context) {
	prod.mu.Lock()
	defer prod.mu.Unlock()

	if !prod.running {
		prod.log.Debug("shutdown called but not running")
		return
	}

	prod.log.Info("shutting down producer")

	PublishGoingDown(ctx, prod.id)

	prod.running = false
	if prod.pubSubResolver != nil {
		prod.pubSubResolver.Clear()
		prod.pubSubResolver = nil
	}

	prod.log.Info("producer shut down complete")

	PublishDown(ctx, prod.id)
}

// IsRunning reports whether the producer is currently running.
func (prod *Producer) IsRunning() bool {
	prod.mu.RLock()
	defer prod.mu.RUnlock()
	return prod.running
}

// ID returns the unique identifier of the producer.
func (prod *Producer) ID() string { return prod.id }

// Produce publishes a message to its configured destination.
func (prod *Producer) Produce(ctx context.Context, m *publicmessage.ProducibleMessage) ([]string, error) {
	prod.mu.RLock()
	running := prod.running
	resolver := prod.pubSubResolver
	prod.mu.RUnlock()

	if !running {
		prod.log.Warn("produce called but producer not running")
		return nil, publicproducer.ErrNotRunning
	}

	if queueParams := m.Queue(); queueParams != nil {
		prod.log.Debug("producing to queue", "queue", queueParams.String())
		return prod.produceToQueue(ctx, m, queueParams, resolver)
	}

	exchangeParams := m.Exchange()
	if exchangeParams == nil {
		prod.log.Warn("produce called without queue or exchange")
		return nil, publicproducer.ErrExchangeRequired
	}

	prod.log.Debug("producing to exchange",
		"exchange", exchangeParams.String(),
		"type", exchangeParams.Type().String(),
		"routingKey", m.ExchangeRoutingKey(),
	)
	return prod.produceToExchange(ctx, m, exchangeParams, resolver)
}

// produceToQueue publishes a message to a specific queue, handling Pub/Sub groups if needed.
func (prod *Producer) produceToQueue(
	ctx context.Context,
	m *publicmessage.ProducibleMessage,
	queueParams *queue.Params,
	resolver *PubSubTargetResolver,
) ([]string, error) {
	var targets []string
	if resolver != nil {
		targets = resolver.Resolve(queueParams)
	}

	if len(targets) > 0 {
		prod.log.Debug("resolved pub/sub targets",
			"queue", queueParams.String(),
			"groups", len(targets),
		)
		ids := make([]string, 0, len(targets))
		for _, groupID := range targets {
			envelope := internalMessage.NewEnvelope(m)
			envelope.SetConsumerGroupID(groupID)

			id, err := prod.dispatch(ctx, envelope, queueParams)
			if err != nil {
				prod.log.Error("failed to produce to consumer group",
					"queue", queueParams.String(),
					"group", groupID,
					"error", err,
				)
				return nil, fmt.Errorf("produce to consumer group %s: %w", groupID, err)
			}
			ids = append(ids, id)
		}
		prod.log.Debug("produced to pub/sub targets",
			"queue", queueParams.String(),
			"messageIDs", len(ids),
		)
		return ids, nil
	}

	envelope := internalMessage.NewEnvelope(m)
	id, err := prod.dispatch(ctx, envelope, queueParams)
	if err != nil {
		prod.log.Error("failed to produce to queue",
			"queue", queueParams.String(),
			"error", err,
		)
		return nil, err
	}

	prod.log.Debug("produced to queue",
		"queue", queueParams.String(),
		"messageID", id,
	)
	return []string{id}, nil
}

// produceToExchange publishes a message to all queues matched by an exchange.
func (prod *Producer) produceToExchange(
	ctx context.Context,
	m *publicmessage.ProducibleMessage,
	exchangeParams *publicexchange.Params,
	resolver *PubSubTargetResolver,
) ([]string, error) {
	queues, err := prod.matchExchangeQueues(ctx, exchangeParams, m.ExchangeRoutingKey())
	if err != nil {
		prod.log.Error("failed to match exchange queues",
			"exchange", exchangeParams.String(),
			"error", err,
		)
		return nil, err
	}

	if len(queues) == 0 {
		prod.log.Warn("no matching queues for exchange",
			"exchange", exchangeParams.String(),
			"routingKey", m.ExchangeRoutingKey(),
		)
		return nil, publicproducer.ErrNoMatchingQueues
	}

	prod.log.Debug("matched exchange queues",
		"exchange", exchangeParams.String(),
		"queues", len(queues),
	)

	var ids []string
	for _, qp := range queues {
		queueIDs, err := prod.produceToQueue(ctx, m, &qp, resolver)
		if err != nil {
			prod.log.Error("failed to produce to matched queue",
				"exchange", exchangeParams.String(),
				"queue", qp.String(),
				"error", err,
			)
			return nil, fmt.Errorf("produce to queue %s: %w", qp.Name(), err)
		}
		ids = append(ids, queueIDs...)
	}

	return ids, nil
}

// matchExchangeQueues resolves the destination queues for an exchange and routing key.
func (prod *Producer) matchExchangeQueues(
	ctx context.Context,
	exchangeParams *publicexchange.Params,
	routingKey string,
) ([]queue.Params, error) {
	switch exchangeParams.Type() {
	case publicexchange.TypeDirect:
		if routingKey == "" {
			return nil, publicproducer.ErrRoutingKeyRequired
		}
		return prod.directExchange.MatchQueues(ctx, exchangeParams, routingKey)

	case publicexchange.TypeTopic:
		if routingKey == "" {
			return nil, publicproducer.ErrRoutingKeyRequired
		}
		return prod.topicExchange.MatchQueues(ctx, exchangeParams, routingKey)

	case publicexchange.TypeFanout:
		return prod.fanoutExchange.MatchQueues(ctx, exchangeParams)

	default:
		return nil, fmt.Errorf("unsupported exchange type: %s", exchangeParams.Type())
	}
}

// dispatch publishes a message envelope to the destination queue.
func (prod *Producer) dispatch(
	ctx context.Context,
	envelope *internalMessage.Envelope,
	queueParams *queue.Params,
) (string, error) {
	envelope.SetDestinationQueue(queueParams)
	messageID := envelope.ID()

	nextScheduledTimestamp := envelope.NextScheduledTimestamp()
	isScheduled := nextScheduledTimestamp > 0

	state := envelope.MessageState()
	now := time.Now().UnixMilli()
	if isScheduled {
		envelope.SetStatus(publicmessage.StatusScheduled)
		state.SetScheduledAt(nextScheduledTimestamp)
		state.SetLastScheduledAt(now)
		state.IncrScheduledTimes()
	} else {
		envelope.SetStatus(publicmessage.StatusPending)
		state.SetPublishedAt(now)
	}

	qKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}
	msgKey := keys.System{}.Message(messageID)

	luaKeys := []string{
		qKey.Properties(),
		qKey.Priority(),
		qKey.Pending(),
		qKey.Scheduled(),
		qKey.Published(),
		qKey.ConsumerGroups(),
		msgKey,
	}

	argv := BuildPublishArgs(envelope)

	reply, err := redisClient.Eval(ctx, scripts.PublishMessage, luaKeys, argv...)
	if err != nil {
		prod.log.Error("publish script failed",
			"messageID", messageID,
			"queue", queueParams.String(),
			"error", err,
		)
		return "", fmt.Errorf("publish message %s: %w", messageID, err)
	}

	replyStr, err := redisClient.String(reply)
	if err != nil {
		prod.log.Error("failed to parse publish reply",
			"messageID", messageID,
			"error", err,
		)
		return "", fmt.Errorf("publish message %s: %w", messageID, err)
	}

	switch replyStr {
	case "OK":
		if !isScheduled {
			PublishMessagePublished(ctx, messageID, *queueParams, prod.id)
		}
		prod.log.Debug("message published",
			"messageID", messageID,
			"queue", queueParams.String(),
			"scheduled", isScheduled,
		)
		return messageID, nil
	case "QUEUE_NOT_FOUND":
		prod.log.Warn("queue not found", "queue", queueParams.String())
		return "", queue.ErrNotFound
	case "CONSUMER_GROUP_NOT_FOUND":
		prod.log.Warn("consumer group not found",
			"queue", queueParams.String(),
			"group", envelope.ConsumerGroupID(),
		)
		return "", publicproducer.ErrConsumerGroupNotFound
	case "MESSAGE_PRIORITY_REQUIRED":
		prod.log.Warn("message priority required", "queue", queueParams.String())
		return "", publicproducer.ErrPriorityRequired
	case "MESSAGE_ALREADY_EXISTS":
		prod.log.Warn("message already exists", "messageID", messageID)
		return "", publicproducer.ErrMessageAlreadyExists
	case "PRIORITY_QUEUING_NOT_ENABLED":
		prod.log.Warn("priority queuing not enabled", "queue", queueParams.String())
		return "", publicproducer.ErrPriorityNotEnabled
	case "UNKNOWN_QUEUE_TYPE":
		prod.log.Warn("unknown queue type", "queue", queueParams.String())
		return "", publicproducer.ErrUnknownQueueType
	case "QUEUE_STOPPED":
		prod.log.Warn("queue stopped", "queue", queueParams.String())
		return "", queue.ErrNotOperational
	case "QUEUE_LOCKED":
		prod.log.Warn("queue locked", "queue", queueParams.String())
		return "", queue.ErrLocked
	case "QUEUE_INVALID_STATE":
		prod.log.Warn("queue in invalid state", "queue", queueParams.String())
		return "", publicproducer.ErrInvalidQueueState
	default:
		prod.log.Error("unexpected publish reply",
			"messageID", messageID,
			"reply", replyStr,
		)
		return "", fmt.Errorf("unexpected script reply: %s", replyStr)
	}
}

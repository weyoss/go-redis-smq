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

	// ctx/cancel derive from the caller's ctx in Run. Shutdown cancels ctx
	// to signal the watcher goroutine; the watcher also exits if the
	// caller's outer ctx is cancelled. Watching a derived context (rather
	// than the outer ctx directly) is what lets Shutdown detach the
	// watcher from the caller's lifecycle — pre-fix, the watcher watched
	// the outer ctx directly and leaked if Shutdown was called without
	// cancelling that ctx (every test using defer-producer.Shutdown
	// leaked one watcher per producer).
	ctx    context.Context
	cancel context.CancelFunc

	// done is closed by the watcher goroutine when its cleanup completes.
	// Shutdown captures done under the lock and waits on it before
	// returning, so by the time Shutdown returns, the watcher has finished
	// its publish calls (PublishGoingDown / PublishDown — Redis ops). This
	// closes a C9-class crash vector: pre-fix, Shutdown returned without
	// waiting, and a subsequent internalredis.Close could race the
	// watcher's in-flight publish and panic in an unrecovered goroutine.
	//
	// Set to a fresh channel per successful Run; closed by the watcher.
	// Subsequent Run calls overwrite it.
	done chan struct{}
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
//
// Run is transactional: on any failure, it publishes the matching
// PublishGoingDown event to close the PublishGoingUp pair opened at
// entry, then returns. No watcher goroutine is spawned on failure, so
// there's no done channel to close and no cleanup to wait for — Run has
// nothing to roll back because the only resource it acquires on the
// failure path is the pubSubResolver, which is nulled before return.
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
		// Close the lifecycle event pair. Pre-fix, subscribers saw
		// "producer.goingUp" with no matching "producer.goingDown" for
		// this failure — dangling event.
		PublishGoingDown(ctx, prod.id)
		prod.log.Error("failed to load pub/sub targets", "error", err)
		return fmt.Errorf("producer: load pub/sub targets: %w", err)
	}

	prod.ctx, prod.cancel = context.WithCancel(ctx)
	prod.running = true
	done := make(chan struct{})
	prod.done = done

	go func() {
		defer close(done)
		<-prod.ctx.Done()
		// Watcher cleanup. Runs if prod.ctx is cancelled — either by
		// Shutdown (primary path) or by the outer ctx's cancellation
		// propagating through. Cleanup is idempotent — it guards on
		// prod.running under prod.mu.
		//
		// No recover() here. PublishGoingDown / PublishDown are
		// Redis-touching (publish through eventmultiplexer → eventbus →
		// Redis PUBLISH). Pre-PR-3, this was a crash vector: Shutdown
		// returned without waiting, and internalredis.Close could nil
		// the client mid-publish. Post-PR-3, Shutdown waits on done, so
		// by the time redissmq.Shutdown reaches internalredis.Close,
		// this goroutine has exited. The wait is the fix — a recover()
		// here would mask a failure of the wait discipline.
		prod.cleanup(context.Background())
	}()

	prod.log.Info("producer started")

	PublishUp(ctx, prod.id)

	return nil
}

// Shutdown gracefully stops the producer.
//
// Two-phase shutdown:
//
//  1. Under prod.mu: capture done (for the wait), release the lock.
//  2. Outside prod.mu: run cleanup (which cancels prod.ctx, publishes
//     the down events, clears the resolver), then wait on done for the
//     watcher goroutine to exit.
//
// Holding prod.mu across <-done would deadlock: the watcher's cleanup
// acquires prod.mu (to clear pubSubResolver). The lock is therefore
// released between phase 1 and the wait.
//
// The ctx parameter is used as the caller-supplied context for the
// publish calls made from the Shutdown goroutine. The watcher's copy of
// the cleanup uses context.Background() (it doesn't have access to the
// caller's ctx — it was spawned at Run time). Either goroutine may win
// the cleanup race; whichever does, its publish calls use its own ctx.
// Both are visible to subscribers by the time Shutdown returns.
//
// Idempotent: safe to call multiple times, and safe to call after the
// outer ctx has already cancelled the producer (in which case the
// watcher has likely already run cleanup; the second call sees
// running == false and waits on the closed done — returns immediately).
func (prod *Producer) Shutdown(ctx context.Context) {
	prod.mu.Lock()
	if !prod.running {
		done := prod.done
		prod.mu.Unlock()
		if done != nil {
			<-done
		}
		return
	}
	done := prod.done
	prod.mu.Unlock()

	prod.cleanup(ctx)

	if done != nil {
		<-done
	}
}

// cleanup performs the one-shot teardown: cancel the derived ctx, publish
// the down events, clear the resolver. Guarded by prod.running so exactly
// one caller (Shutdown goroutine OR watcher goroutine) runs the work.
func (prod *Producer) cleanup(ctx context.Context) {
	prod.mu.Lock()
	if !prod.running {
		prod.mu.Unlock()
		return
	}
	prod.running = false
	if prod.cancel != nil {
		prod.cancel() // wakes the watcher; it'll see running == false and no-op
		prod.cancel = nil
	}
	prod.mu.Unlock()

	PublishGoingDown(ctx, prod.id)

	// Acquiring Lock here blocks until every in-flight Produce has
	// released its RLock. Once we hold it, no Produce is using the
	// resolver — clear it safely. See Produce's doc for the invariant.
	prod.mu.Lock()
	if prod.pubSubResolver != nil {
		prod.pubSubResolver.Clear()
		prod.pubSubResolver = nil
	}
	prod.mu.Unlock()

	PublishDown(ctx, prod.id)

	prod.log.Info("producer shut down complete")
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
	// RLock is held for the entire produce operation. cleanup acquires
	// Lock, which blocks until every in-flight Produce has released its
	// RLock — guaranteeing that:
	//
	//   1. cleanup cannot clear pubSubResolver while a Produce is using
	//      it. Pre-fix, Produce captured resolver under a brief RLock,
	//      then released before doing Redis I/O. A concurrent cleanup
	//      could clear the resolver in that window; Resolve() would
	//      return (nil, false), which makes Produce dispatch to the
	//      queue-level Pending list. On a Pub/Sub queue that list is
	//      never read — the message was silently orphaned. This is the
	//      exact class of bug ErrNoConsumerGroups exists to prevent.
	//
	//   2. Shutdown cannot return while a Produce is still calling
	//      internalredis.Client() / Eval. Shutdown blocks on cleanup,
	//      cleanup blocks on the Lock, which blocks on every in-flight
	//      RLock. By the time Shutdown returns, no produce goroutine can
	//      touch the Redis client — so internalredis.Close cannot panic
	//      an in-flight produce.
	//
	// Holding RLock across Redis I/O is the same discipline used by
	// consumer.MessageHandler and queue.PurgeWorker: an unbounded wait,
	// bounded in practice by the Redis client's own dial/read/write
	// timeouts. A bounded wait would reintroduce both races.
	defer prod.mu.RUnlock()
	prod.mu.RLock()

	if !prod.running {
		prod.log.Warn("produce called but producer not running")
		return nil, publicproducer.ErrNotRunning
	}

	if queueParams := m.Queue(); queueParams != nil {
		prod.log.Debug("producing to queue", "queue", queueParams.String())
		return prod.produceToQueue(ctx, m, queueParams, prod.pubSubResolver)
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
	return prod.produceToExchange(ctx, m, exchangeParams, prod.pubSubResolver)
}

// resolvePubSubTargets returns the consumer groups to publish to for the
// given queue, plus whether it's a Pub/Sub queue. If the resolver's cached
// state says "Pub/Sub with zero groups," a fresh Redis read confirms before
// returning — the resolver is populated by consumerGroupCreated events and
// may be briefly stale after a group is created but before the event has
// been processed.
//
// Returns ErrNoConsumerGroups if the queue is a Pub/Sub queue and genuinely
// has zero groups.
func (prod *Producer) resolvePubSubTargets(
	ctx context.Context,
	queueParams *queue.Params,
	resolver *PubSubTargetResolver,
) (targets []string, isPubSub bool, err error) {
	if resolver == nil {
		return nil, false, nil
	}

	groups, isPubSub := resolver.Resolve(queueParams)
	if !isPubSub || len(groups) > 0 {
		return groups, isPubSub, nil
	}

	// Resolver says "Pub/Sub, zero groups" — but its cache may be stale.
	// Confirm against Redis before rejecting.
	qKey := keys.Queue{Namespace: queueParams.NS(), Name: queueParams.Name()}
	client := redisClient.Client()

	exists, err := client.Exists(ctx, qKey.Properties()).Result()
	if err != nil {
		return nil, isPubSub, fmt.Errorf("check queue exists: %w", err)
	}
	if exists == 0 {
		return nil, isPubSub, queue.ErrNotFound
	}

	freshGroups, err := redisClient.LoadSetMembers(ctx, qKey.ConsumerGroups(), "consumer groups")
	if err != nil {
		return nil, isPubSub, fmt.Errorf("load consumer groups: %w", err)
	}
	if len(freshGroups) == 0 {
		return nil, isPubSub, publicproducer.ErrNoConsumerGroups
	}
	return freshGroups, isPubSub, nil
}

// produceToQueue publishes a message to a specific queue, handling Pub/Sub
// groups if needed.
func (prod *Producer) produceToQueue(
	ctx context.Context,
	m *publicmessage.ProducibleMessage,
	queueParams *queue.Params,
	resolver *PubSubTargetResolver,
) ([]string, error) {
	targets, isPubSub, err := prod.resolvePubSubTargets(ctx, queueParams, resolver)
	if err != nil {
		return nil, err
	}

	if isPubSub {
		// resolvePubSubTargets guarantees targets is non-empty when isPubSub
		// is true (ErrNoConsumerGroups is returned otherwise).
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

	// Pre-flight: validate every matched queue before dispatching any. If a
	// queue is Pub/Sub with no consumer groups, reject the whole produce
	// rather than partially succeeding (some queues got the message, one
	// queue caused the error mid-loop). Partial success is worse than an
	// up-front error — the caller would see an error but also some queues
	// would have the message, and a retry would produce duplicates.
	for i := range queues {
		if _, _, err := prod.resolvePubSubTargets(ctx, &queues[i], resolver); err != nil {
			return nil, fmt.Errorf("produce to queue %s: %w", queues[i].Name(), err)
		}
	}

	prod.log.Debug("matched exchange queues",
		"exchange", exchangeParams.String(),
		"queues", len(queues),
	)

	var ids []string
	for i := range queues {
		queueIDs, err := prod.produceToQueue(ctx, m, &queues[i], resolver)
		if err != nil {
			// Should not happen — pre-flight already validated every queue.
			// Log and abort defensively in case of a state change between
			// pre-flight and dispatch (e.g., group deleted by another actor).
			prod.log.Error("failed to produce to matched queue",
				"exchange", exchangeParams.String(),
				"queue", queues[i].String(),
				"error", err,
			)
			return nil, fmt.Errorf("produce to queue %s: %w", queues[i].Name(), err)
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

	priorityKey := qKey.Priority()
	pendingKey := qKey.Pending()
	if envelope.ConsumerGroupID() != "" {
		priorityKey = qKey.PriorityWithGroup(envelope.ConsumerGroupID())
		pendingKey = qKey.PendingWithGroup(envelope.ConsumerGroupID())
	}

	luaKeys := []string{
		qKey.Properties(),
		priorityKey,
		pendingKey,
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

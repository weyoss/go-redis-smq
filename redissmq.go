/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package redissmq is the entry point for the RedisSMQ message queue.
//
// It provides bootstrap and lifecycle management (Init/Shutdown) for the
// RedisSMQ runtime, as well as factory functions that return the concrete
// implementations behind the public interfaces defined in the pkg packages.
//
// The package is designed to be imported only by the application's main
// package or composition root. All other packages should depend on the
// public interfaces rather than this package.
package redissmq

import (
	"context"
	"fmt"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	internalconfig "github.com/weyoss/go-redis-smq/internal/config"
	internalconsumer "github.com/weyoss/go-redis-smq/internal/consumer"
	internaleventbus "github.com/weyoss/go-redis-smq/internal/eventbus"
	internalexchange "github.com/weyoss/go-redis-smq/internal/exchange"
	internalmessage "github.com/weyoss/go-redis-smq/internal/message"
	internalnamespace "github.com/weyoss/go-redis-smq/internal/namespace"
	internalproducer "github.com/weyoss/go-redis-smq/internal/producer"
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	internalredis "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/eventbus"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/namespace"
	"github.com/weyoss/go-redis-smq/pkg/producer"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

var (
	// systemCtx holds the root context for the RedisSMQ runtime.
	systemCtx context.Context
	// systemStop cancels the root context.
	systemStop context.CancelFunc
	// purgeWorkerStop stops the background purge worker.
	purgeWorkerStop func()
	purgeWorker     *internalqueue.PurgeWorker

	lifecycleMu sync.Mutex
	initialized bool
)

var log = logger.New("redissmq")

type trackedInstances struct {
	mu        sync.Mutex
	producers []producer.Producer
	consumers []consumer.Consumer
}

var instances trackedInstances

func registerProducer(p producer.Producer) {
	instances.mu.Lock()
	defer instances.mu.Unlock()
	instances.producers = append(instances.producers, p)
}

func registerConsumer(c consumer.Consumer) {
	instances.mu.Lock()
	defer instances.mu.Unlock()
	instances.consumers = append(instances.consumers, c)
}

// userBusAdapter adapts the internal event bus to the public event bus interface.
type userBusAdapter struct {
	bus *internaleventbus.EventBus
}

// Subscribe implements eventbus.EventBus.
func (a *userBusAdapter) Subscribe(handler func(eventName string, args []interface{}), eventName string) (eventbus.Subscription, error) {
	sub, err := a.bus.Subscribe(handler, eventName)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// Init initializes the RedisSMQ runtime with the given Redis client.
//
// The client must be a single-node Redis client (*redis.Client). Cluster and
// ring clients are rejected because RedisSMQ relies on multi-key Lua scripts
// that require all keys to reside on a single Redis node.
//
// It starts the internal system event bus, loads the
// configuration, and launches background workers (such as the queue purge
// worker). The provided context is used as the parent for all internal
// operations; cancelling it triggers a graceful shutdown.
//
// Init is idempotent and can be called multiple times; subsequent calls
// are no-ops.
func Init(ctx context.Context, client goredis.UniversalClient) error {
	if _, ok := client.(*goredis.Client); !ok {
		return fmt.Errorf("redissmq: only *redis.Client is supported; cluster and ring clients are not allowed")
	}

	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	if initialized {
		return nil
	}

	if err := internalredis.Init(ctx, client); err != nil {
		return fmt.Errorf("redissmq: redis init failed: %w", err)
	}

	internaleventbus.InitSystem(ctx)

	if err := internalconfig.Init(ctx); err != nil {
		return fmt.Errorf("redissmq: config init failed: %w", err)
	}

	systemCtx, systemStop = context.WithCancel(ctx)

	// Start the purge worker.
	purgeManager := internalqueue.NewManager().Purge()
	purgeWorker = internalqueue.NewPurgeWorker(purgeManager)
	purgeWorkerStop = purgeWorker.Start(systemCtx)

	go func() {
		<-ctx.Done()
		Shutdown()
	}()

	initialized = true
	log.Info("RedisSMQ initialized successfully")
	return nil
}

// InitUserEventBus starts the public user event bus.
//
// The user event bus is separate from the internal system bus and is used
// to deliver events to external subscribers via the public subscription
// functions in the pkg packages. It is not started automatically; call this
// function if you need to use the public event subscription API.
func InitUserEventBus(ctx context.Context) {
	bus := internaleventbus.InitUser(ctx)
	eventbus.SetUserBus(&userBusAdapter{bus: bus})
}

// ShutdownUserEventBus stops the public user event bus and clears the global
// reference. It can be called independently of the main Shutdown function.
func ShutdownUserEventBus() {
	eventbus.SetUserBus(nil)
	internaleventbus.ShutdownUser()
}

// Shutdown gracefully stops the RedisSMQ runtime.
//
// It stops the purge worker, shuts down all registered consumers and
// producers, stops the event buses, releases the configuration, and clears
// internal Redis references. The Redis client itself is not closed; the
// caller remains responsible for closing it.
//
// Shutdown is safe to call multiple times and may be followed by another
// Init to restart the runtime.
func Shutdown() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	if !initialized {
		return
	}

	log.Info("RedisSMQ shutting down...")

	if purgeWorkerStop != nil {
		purgeWorkerStop()
		purgeWorkerStop = nil
		purgeWorker = nil
	}

	if systemStop != nil {
		systemStop()
		systemStop = nil
		systemCtx = nil
	}

	instances.mu.Lock()
	consumers := make([]consumer.Consumer, len(instances.consumers))
	copy(consumers, instances.consumers)
	producers := make([]producer.Producer, len(instances.producers))
	copy(producers, instances.producers)
	instances.consumers = nil
	instances.producers = nil
	instances.mu.Unlock()

	for _, c := range consumers {
		c.Shutdown()
	}
	log.Info("consumers shut down", "count", len(consumers))

	bgCtx := context.Background()
	for _, p := range producers {
		p.Shutdown(bgCtx)
	}
	log.Info("producers shut down", "count", len(producers))

	ShutdownUserEventBus()
	internaleventbus.ShutdownSystem()

	log.Info("RedisSMQ shut down complete")

	logger.Shutdown()
	internalconfig.Close()
	internalredis.Close()

	initialized = false
}

// NewProducer returns a new producer that implements producer.Producer.
// The producer is automatically registered for lifecycle management and will
// be shut down when Shutdown is called.
func NewProducer() producer.Producer {
	p := internalproducer.New()
	registerProducer(p)
	return p
}

// NewConsumer returns a new consumer that implements consumer.Consumer.
// The consumer is automatically registered for lifecycle management and will
// be shut down when Shutdown is called.
func NewConsumer(opts ...consumer.Option) consumer.Consumer {
	c := internalconsumer.New(opts...)
	registerConsumer(c)
	return c
}

// NewQueueManager returns a new queue manager that implements
// queue.Manager.
func NewQueueManager() queue.Manager {
	return internalqueue.NewManager()
}

// NewStateManager returns a new state manager that implements
// queue.StateManager.
func NewStateManager() queue.StateManager {
	return internalqueue.NewStateManager()
}

// NewConsumerGroupManager returns a new consumer group manager that
// implements queue.ConsumerGroupManager.
func NewConsumerGroupManager() queue.ConsumerGroupManager {
	return internalqueue.NewConsumerGroupManager()
}

// NewMessageManager returns a new message manager that implements
// message.Manager.
func NewMessageManager() message.Manager {
	return internalmessage.NewManager()
}

// NewExchangeManager returns a new exchange manager that implements
// exchange.Manager.
func NewExchangeManager() exchange.Manager {
	return internalexchange.NewManager()
}

// NewDirectExchange returns a new direct exchange that implements
// exchange.DirectExchange.
func NewDirectExchange() exchange.DirectExchange {
	return internalexchange.NewManager().Direct()
}

// NewFanoutExchange returns a new fanout exchange that implements
// exchange.FanoutExchange.
func NewFanoutExchange() exchange.FanoutExchange {
	return internalexchange.NewManager().Fanout()
}

// NewTopicExchange returns a new topic exchange that implements
// exchange.TopicExchange.
func NewTopicExchange() exchange.TopicExchange {
	return internalexchange.NewManager().Topic()
}

// NewConfigManager returns the singleton configuration manager that implements
// config.Manager.
func NewConfigManager() config.Manager {
	return internalconfig.DefaultManager()
}

// NewNamespaceManager returns a new namespace manager that implements
// namespace.Manager.
func NewNamespaceManager() namespace.Manager {
	return internalnamespace.NewManager()
}

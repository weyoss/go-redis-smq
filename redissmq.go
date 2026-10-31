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
	// generation is incremented on each successful Init. It identifies the
	// runtime instance. Watchers spawned by Init capture the generation they
	// were created for and pass it to shutdownIfCurrent, so a stale watcher
	// can detect that the runtime it was watching has been superseded by a
	// later Init and bail out instead of tearing down the new runtime.
	generation uint64
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
//
// On failure, Init unwinds any partial initialization via shutdownLocked
// before returning. The runtime is left in exactly the same state it was in
// before the call — safe to retry with a fresh client (or a corrected
// config). A precondition failure (wrong client type) returns before any
// state is touched.
func Init(ctx context.Context, client goredis.UniversalClient) error {
	// Precondition failure — nothing has been initialized, so there is
	// nothing to unwind. Return the error without touching runtime state.
	if _, ok := client.(*goredis.Client); !ok {
		return fmt.Errorf("redissmq: only *redis.Client is supported; cluster and ring clients are not allowed")
	}

	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	if initialized {
		return nil
	}

	// Re-enable logging in case a previous Init failed and its rollback
	// closed the logger via shutdownLocked's logger.Shutdown() call.
	// Without this, a retry would run silently even if the user's config
	// has Logger.Enabled = true. Idempotent when already open.
	logger.Init()

	if err := internalredis.Init(ctx, client); err != nil {
		shutdownLocked()
		return fmt.Errorf("redissmq: redis init failed: %w", err)
	}

	internaleventbus.InitSystem(ctx)

	if err := internalconfig.Init(ctx); err != nil {
		shutdownLocked()
		return fmt.Errorf("redissmq: config init failed: %w", err)
	}

	systemCtx, systemStop = context.WithCancel(ctx)

	// Bump the generation and capture it for the watcher below. The watcher
	// calls shutdownIfCurrent(gen) — not Shutdown() — so that a stale
	// watcher (whose Init has already been torn down and superseded by a
	// later Init) can detect the mismatch and bail out instead of tearing
	// down the later Init's runtime.
	//
	// Without this check: if the outer ctx of Init #1 is cancelled after
	// Init #1 was already Shutdown() and a NEW Init #2 has run, Init #1's
	// leaked watcher would acquire lifecycleMu (possibly after Init #2
	// finished), see initialized==true, and call Shutdown() — killing
	// Init #2's runtime.
	generation++
	watchedGen := generation
	watchedCtx := systemCtx

	// Start the purge worker.
	purgeManager := internalqueue.NewManager().Purge()
	purgeWorker = internalqueue.NewPurgeWorker(purgeManager)
	purgeWorkerStop = purgeWorker.Start(systemCtx)

	go func() {
		<-watchedCtx.Done()
		shutdownIfCurrent(watchedGen)
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
// Shutdown is idempotent: it may be called any number of times, before Init
// (as a no-op cleanup), after a failed Init (redundant — Init already
// unwound via shutdownLocked — but safe), or after a successful Init (the
// normal path). After Shutdown, the runtime may be re-initialized with
// another call to Init.
func Shutdown() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	shutdownLocked()
}

// shutdownIfCurrent is called by the watcher goroutine spawned in Init.
//
// It shuts down the runtime only if the current generation matches gen —
// i.e., only if the runtime it was watching is still the active one.
// Otherwise a later Init has superseded it, and touching the new runtime
// would corrupt live state.
//
// The initialized check is redundant given the generation check would fail
// on the same interleavings, but keeping it makes the intent explicit: the
// watcher has no business calling into shutdownLocked if there is no live
// runtime at all.
func shutdownIfCurrent(gen uint64) {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	if !initialized {
		return
	}
	if generation != gen {
		return
	}
	shutdownLocked()
}

// shutdownLocked performs the shutdown sequence. Caller must hold
// lifecycleMu.
//
// Idempotent: every cleanup step is nil-safe or an atomic swap. Calling it
// on a partially-initialized runtime (after an Init failure) unwinds exactly
// what was done and no more. Calling it twice in a row is harmless.
//
// The wasInit flag only affects log verbosity — a rollback after a failed
// Init logs at Debug rather than Info, so a failed-then-retried Init doesn't
// produce "RedisSMQ shutting down..." messages on the user's console.
func shutdownLocked() {
	wasInit := initialized

	if wasInit {
		log.Info("RedisSMQ shutting down...")
	} else {
		log.Debug("RedisSMQ rolling back partial initialization...")
	}

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
	for _, p := range producers {
		p.Shutdown(context.Background())
	}

	ShutdownUserEventBus()
	internaleventbus.ShutdownSystem()

	if wasInit {
		log.Info("RedisSMQ shut down complete")
	} else {
		log.Debug("RedisSMQ rollback complete")
	}

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

# Graceful Shutdown

RedisSMQ handles shutdowns without losing messages. In-flight messages are
recovered and returned to the pending queue.

## System Shutdown

RedisSMQ does not own the Redis client; you provide it when calling
`redissmq.Init`. After `Shutdown()`, RedisSMQ releases internal references
to the client, but it does **not** close it. You are responsible for
closing the client when your application is completely finished with
Redis.

```go
func main() {
    ctx := context.Background()

    // Create a single-node Redis client.
    rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
    defer rdb.Close() // close AFTER RedisSMQ shutdown

    if err := redissmq.Init(ctx, rdb); err != nil {
        log.Fatal(err)
    }
    defer redissmq.Shutdown()

    // ... use producers and consumers ...
}
```

Because `defer` executes in LIFO order, `redissmq.Shutdown()` runs before
`rdb.Close()`, which is the correct sequence.

`redissmq.Shutdown()` shuts down in order:

1. **Purge worker** — drains any in-flight purge job, then exits. Any queue
   it was purging is unlocked as part of the job's completion or failure
   path before the worker exits.
2. **Worker coordinator** — cancels all per-queue worker sets (scheduled
   publisher, delayed requeuer, immediate requeuer, dead-consumer reaper,
   orphaned lock recoverer), waits for in-flight work to complete, and
   releases the per-queue `WorkersLock` leases it held. By the time this
   step returns, no coordinator-owned goroutine is calling Redis.
3. All consumers — in-flight messages returned to pending.
4. All producers — pending publishes complete.
5. Configuration manager.
6. Event buses (system and user, if started).
7. Internal Redis references (client remains open for the caller).

## Background workers

`redissmq.Init` starts two process-scoped workers. They run in the
background for the lifetime of the runtime and shut down as part of
`redissmq.Shutdown()`. You do not manage them directly.

### Purge worker

Executes queue-purge jobs from `PendingPurgeJobs`. One job per process at a
time; the job's batches are deleted with configurable delay between them.

### Worker coordinator

Runs the five queue-scoped background workers:

| Worker | Cadence | What it does |
|--------|---------|--------------|
| Scheduled publisher | 5s | Moves due scheduled messages (delays, CRON, repeats) into pending. |
| Delayed requeuer | 5s | Moves due delayed messages (retry backoff) into pending. |
| Immediate requeuer | 5s | Moves requeued messages into pending (or delayed if a retry delay is set). |
| Dead-consumer reaper | 30s | Detects consumers whose heartbeat expired and recovers their in-flight messages. |
| Orphaned lock recoverer | 30s | Unlocks queues whose purge worker died mid-job. |

The coordinator elects **one holder per queue across the cluster** using
the per-queue `WorkersLock`. Exactly one process runs the five workers for
a given queue at a time; other processes leave it alone until the holder's
lease expires or is released. This gives you:

- Exactly-once scheduled and requeued message transitions per tick.
- Cross-process failover: if the holder crashes, its lease expires after
  30 seconds and another process picks up the queue within one reconcile
  tick (30 seconds).
- No per-consumer stuck goroutines: losers don't block or retry, they just
  wait for the next reconcile cycle.

### Timing

The coordinator scans `AllQueues()` every 30 seconds. A queue created *after*
`redissmq.Init` returns is picked up on the next reconcile tick — up to 30
seconds before its scheduled messages start moving. Queues that exist at
`Init` time are handled immediately (the first reconcile pass is synchronous
before `Init` returns).

If a lease is lost (holder crashes, or its `ExtendLock` refresh is rejected
because the TTL expired during a network blip), the holder cancels its
workers within one refresh interval (10 seconds). The next reconcile tick
on any process picks up the queue. The split-brain window — two processes
running workers for the same queue — is bounded at ≤10 seconds and is
neutralized by the underlying Lua scripts' per-message gating (`ZREM`/`LREM`
checks that make every transition idempotent).

## Individual Shutdown

```go
// Shut down a specific consumer
consumer.Shutdown()

// Shut down a specific producer
producer.Shutdown(ctx)
```

Stopping a consumer does **not** stop the coordinator or the purge worker.
Those are process-scoped and only exit on `redissmq.Shutdown()`.

## Signal Handling

```go
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
    defer rdb.Close()

    if err := redissmq.Init(ctx, rdb); err != nil {
        log.Fatal(err)
    }
    defer redissmq.Shutdown()

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        <-sigCh
        cancel()
    }()

    // ... use producers and consumers with ctx ...
}
```

## Crash Recovery

If a **consumer** crashes without a clean shutdown:

- Its heartbeat stops.
- The dead-consumer reaper (running in another process, elected via the
  queue's `WorkersLock`) detects the dead consumer within 30–60 seconds
  (heartbeat TTL + reconcile cadence).
- In-flight messages are recovered automatically.

If the **process holding a queue's worker lease** crashes:

- Its `WorkersLock` lease expires after 30 seconds.
- Another process acquires the lease on its next reconcile tick (≤30 seconds
  after expiry).
- The five workers resume on that queue.

In both cases, no messages are lost.

## Best Practices

- Use `defer redissmq.Shutdown()` **before** `defer rdb.Close()` so
  RedisSMQ releases internal resources first.
- Handle OS signals for graceful shutdown.
- Don't force exit — let cleanup complete. `redissmq.Shutdown()` blocks
  until both process-scoped workers have fully drained; the worst case is
  bounded by in-flight Redis operations (dial/read/write timeouts), not by
  queue size or the number of live consumers.
- Shut down RedisSMQ before closing the Redis client.
- If you started the public event bus with `redissmq.InitUserEventBus(ctx)`,
  it is automatically stopped by `redissmq.Shutdown()`; you do not need to
  call `redissmq.ShutdownUserEventBus()` separately unless you want to
  stop it earlier.
- Do not use cluster or ring clients; RedisSMQ requires a single-node Redis
  client.
- **Don't create queues in tight shutdown loops.** A queue created less
  than 30 seconds before shutdown may not have its workers elected yet —
  any scheduled or delayed messages it holds will still be there after a
  restart, waiting for the next coordinator cycle.

## Related

- [Graceful Shutdown Concepts](https://github.com/weyoss/redis-smq-docs) — How shutdown works
- [Scheduling Messages](scheduling-messages.md) — Scheduler cadence and timing
- [Consuming Messages](consuming-messages.md) — Consumer lifecycle

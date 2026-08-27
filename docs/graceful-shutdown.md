# Graceful Shutdown

RedisSMQ handles shutdowns without losing messages. In‑flight messages are recovered and returned to the pending queue.

## System Shutdown

RedisSMQ does not own the Redis client; you provide it when calling `redissmq.Init`. After `Shutdown()`, RedisSMQ releases internal references to the client, but it does **not** close it. You are responsible for closing the client when your application is completely finished with Redis.

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

Because `defer` executes in LIFO order, `redissmq.Shutdown()` runs before `rdb.Close()`, which is the correct sequence.

`redissmq.Shutdown()` shuts down in order:

1. All consumers — in‑flight messages returned to pending
2. All producers — pending publishes complete
3. Configuration manager
4. Event buses (system and user, if started)
5. Internal Redis references (client remains open for the caller)

## Individual Shutdown

```go
// Shut down a specific consumer
consumer.Shutdown()

// Shut down a specific producer
producer.Shutdown(ctx)
```

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

If a consumer crashes without a clean shutdown:

- Heartbeats stop
- A background reaper detects the dead consumer
- In‑flight messages are recovered automatically

No messages are lost.

## Best Practices

- Use `defer redissmq.Shutdown()` **before** `defer rdb.Close()` so RedisSMQ releases internal resources first.
- Handle OS signals for graceful shutdown.
- Don’t force exit — let cleanup complete.
- Shut down RedisSMQ before closing the Redis client.
- If you started the public event bus with `redissmq.InitUserEventBus(ctx)`, it is automatically stopped by `redissmq.Shutdown()`; you do not need to call `redissmq.ShutdownUserEventBus()` separately unless you want to stop it earlier.
- Do not use cluster or ring clients; RedisSMQ requires a single-node Redis client.

## Related

- [Graceful Shutdown Concepts](https://github.com/weyoss/redis-smq-docs) — How shutdown works

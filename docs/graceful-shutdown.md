# Graceful Shutdown

RedisSMQ handles shutdowns without losing messages. In-flight messages are recovered and returned to the pending queue.

## System Shutdown

```go
func main() {
ctx := context.Background()

if err := redissmq.Init(ctx, redissmq.Config{Addr: "127.0.0.1:6379"}); err != nil {
log.Fatal(err)
}
defer redissmq.Shutdown()

// ... use producers and consumers ...
}
```

`redissmq.Shutdown()` shuts down in order:

1. All consumers — in-flight messages returned to pending
2. All producers — pending publishes complete
3. Configuration singleton
4. Event bus
5. Redis connections

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

if err := redissmq.Init(ctx, redissmq.Config{Addr: "127.0.0.1:6379"}); err != nil {
log.Fatal(err)
}
defer redissmq.Shutdown()

sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

go func () {
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
- In-flight messages are recovered automatically

No messages are lost.

## Best Practices

- Use `defer redissmq.Shutdown()` in `main()`
- Handle OS signals for graceful shutdown
- Don't force exit — let cleanup complete
- Shut down RedisSMQ before closing Redis connections

## Related

- [Graceful Shutdown Concepts](../../../docs/graceful-shutdown.md) — How shutdown works
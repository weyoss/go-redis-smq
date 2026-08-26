# Go Documentation

Welcome to the Go implementation of RedisSMQ. These guides cover the public API, configuration, and operational patterns.

For language‑agnostic concepts (architecture, queues, exchanges, reliability, etc.), see the
[RedisSMQ documentation](https://github.com/weyoss/redis-smq-docs).

## Getting Started

- [Quick Start](quick-start.md) — Install, initialize, send and receive your first message
- [Configuration](configuration.md) — System configuration API and manager
- [Graceful Shutdown](graceful-shutdown.md) — Clean shutdown patterns

## Core Operations

- [Producing Messages](producing-messages.md) — How to publish messages
- [Consuming Messages](consuming-messages.md) — How to subscribe and process messages
- [Queue Management](queue-management.md) — Create, inspect, delete queues
- [Queue State Management](queue-state-management.md) — Pause, resume, stop queues
- [Queue Rate Limiting](queue-rate-limiting.md) — Control consumption speed
- [Message Management](message-management.md) — Retrieve, delete, and requeue messages
- [Message Browsing](message-browsing.md) — Browse published, pending, scheduled, and audited messages
- [Exchange Management](exchange-management.md) — Direct, topic, and fanout exchanges
- [Scheduling Messages](scheduling-messages.md) — Delays, CRON, and repeating delivery
- [Consumer Groups](consumer-groups.md) — Pub/Sub consumer groups
- [Namespaces](namespaces.md) — Namespace management

## Events & Monitoring

- [Event Bus](event-bus.md) — Real‑time public system events

## Operations

- [Error Handling](error-handling.md) — Error types and handling patterns

## Architecture

RedisSMQ uses a clean layered architecture:

- **Public packages (`pkg/...`)** — interfaces, types, and documentation only.
- **Internal packages (`internal/...`)** — concrete Redis-backed implementations.
- **Root `redissmq` package** — composition root and factory functions.

All managers, producers, consumers, exchanges, and other components are obtained via factory functions in `redissmq` (e.g., `redissmq.NewQueueManager()`, `redissmq.NewProducer()`). Public packages are designed to be used without importing internal code.

## Additional Resources

- [BUILD.md](../BUILD.md) — build, test, and coverage instructions
- [redis-smq-docs](https://github.com/weyoss/redis-smq-docs) — language-agnostic concepts
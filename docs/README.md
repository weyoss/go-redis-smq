# Go Documentation

Go implementation of RedisSMQ. For concepts that apply to all implementations, see
the [RedisSMQ language‑agnostic documentation](https://github.com/weyoss/redis-smq-docs).

## Getting Started

- [Quick Start](quick-start.md) — Install, initialize, send and receive your first message

## Core Operations

- [Producing Messages](producing-messages.md) — How to publish messages
- [Consuming Messages](consuming-messages.md) — How to subscribe and process messages
- [Queue Management](queue-management.md) — Create, inspect, delete queues
- [Queue State Management](queue-state-management.md) — Pause, resume, stop queues
- [Message Management](message-management.md) — Retrieve, delete, and requeue messages
- [Message Browsing](message-browsing.md) — Browse published, pending, scheduled, and audited messages
- [Exchange Management](exchange-management.md) — Direct, topic, and fanout exchanges
- [Scheduling Messages](scheduling-messages.md) — Delays, CRON, and repeating delivery
- [Consumer Groups](consumer-groups.md) — Pub/Sub consumer groups
- [Namespaces](namespaces.md) — Namespace management
- [Event Bus](event-bus.md) — Real‑time system events

## Operations

- [Configuration](configuration.md) — System configuration API
- [Graceful Shutdown](graceful-shutdown.md) — Clean shutdown patterns
- [Error Handling](error-handling.md) — Error types and handling patterns

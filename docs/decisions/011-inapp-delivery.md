# 011. In-app messages: durable inbox plus sharded pub/sub to WebSocket nodes

**Status:** accepted

## Context
In-app delivery has two jobs: keep a history the user can read later, and show new messages instantly if the user is online. A user's socket lives on one gateway node among many.

## Decision
The worker writes each message to an `inbox` table (the durable record) and publishes it to Redis. Channels are sharded into 64 (`inapp:00` to `inapp:63`, by hash of the user id). A gateway node subscribes to a shard only while it has at least one connected user in it, and routes each message to that user's local sockets. Clients read history through the inbox API with a cursor, and refetch it after reconnecting.

## Why this is better
- **Durable and live are separate.** A missed push never loses a message, because the inbox is the truth.
- **Traffic per node stays proportional to its users.** One subscription per user would not scale, and one global channel would send every message to every node. Sharding bounds both.
- **Replay-safe.** The inbox key derives from the message, so a redelivery overwrites instead of duplicating.
- **Slow clients cannot hurt the node.** A full per-connection buffer drops the message (counted in `inapp_dropped_total`) instead of blocking others.

## What we give up
- Redis pub/sub is fire and forget. Disconnected clients must fetch the inbox on reconnect.
- With 64 shards, a node serving many users subscribes to most of them. The saving shrinks as users per node grow.
- The gateway authenticates with a producer API key, so it is for server-to-server use. Exposing it to mobile clients needs user-scoped tokens first.

## Evidence
Measured: a gateway test (a message reaches only the target user even when users share a shard), a live run where a WebSocket client received a message end to end, and inbox pagination and read-flag tests.

## Revisit when
Gateway nodes hold users from nearly all shards (move to consistent hashing of users to nodes) or message loss while offline is unacceptable beyond the inbox.

# 008. Redis for caches, caps, dedupe, rate limits and live fan-out

**Status:** accepted

## Context
Several hot-path jobs need very fast shared state that is allowed to be lost: user preferences read on every message, per-user daily caps, send claims, provider rate limits, and a way to reach the node that holds a WebSocket.

## Decision
Use Redis for all of them, with no persistence (`appendonly no`). DynamoDB remains the source of truth for anything that must survive.
- **User cache:** 5 minute TTL, read-through, deleted on every write, collapsed with singleflight. Unknown users are never cached.
- **Frequency caps:** a set of notification ids per user, category and day. Adding an id that is already in the set counts as allowed, so a redelivered message is not counted twice.
- **Rate limits:** a token bucket in a Lua script, per channel and priority. Each instance leases a chunk of tokens (up to 100, rate divided by 10) and spends them locally.
- **Dedupe and live fan-out:** see 007 and 011.

## Why this is better
- **Round trips are saved where it counts.** Leasing token chunks keeps the limiter to one Redis call per chunk instead of per send, so the limiter does not itself become the bottleneck at tens of thousands of sends per second.
- **The shared limit is truly shared.** Many worker pods respect one provider quota, which in-process limiters cannot do.
- **The cache cannot go stale for long.** Writes invalidate, TTL bounds any race, and misses are not cached, so a user who registers and is notified a second later is not suppressed.
- **Counting is redelivery-safe.** A set instead of a counter means at-least-once delivery does not eat a user's daily cap.

## What we give up
- **Redis is a hard dependency for sending.** Dedupe and limiting fail closed, so an outage halts delivery (see 007).
- A read-through fill racing an invalidation can serve a stale entry for up to the TTL.
- A bug in how this was first wired (the API's client pointed at the wrong address, so invalidation silently never ran) shows cache invalidation errors are ignored by design. Add a metric if you want to see them.

## Evidence
Measured: integration tests for the cache (misses not cached, writes invalidate), dedupe and the limiter (rate enforced across two instances). Redis clustering and failure were not tested.

## Revisit when
One Redis cannot take the load (shard by key or move limits to a cluster), or fail-closed behaviour is unacceptable for a traffic class.

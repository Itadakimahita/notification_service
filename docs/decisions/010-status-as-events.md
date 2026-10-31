# 010. Delivery status as an event stream with a monotonic rank

**Status:** accepted (with a known scaling concern)

## Context
Many services learn about a notification's progress, some out of order (a provider webhook can arrive before a worker records the send). Writing every attempt to the database on the hot path is expensive and races.

## Decision
Services publish small status events to `notifications.status`. A status service applies them to the notification record. Each status has a rank (scheduled 1, routed or queued 2, failed or suppressed or expired 3, sent 4, delivered 5) and a write only succeeds if its rank is higher than the stored one. Within a batch only the highest event per notification and channel is written.

## Why this is better
- **Order does not matter.** A late `QUEUED` can never overwrite `DELIVERED`, and a duplicate event changes nothing.
- **Replays work.** Rank 3 (failed) sits below rank 4 (sent), so a notification replayed from the DLQ moves from FAILED to SENT to DELIVERED.
- **The hot path stays light.** Workers and the router emit events and move on. Only the latest state per channel is stored.
- **Phantom records are impossible.** Updates require the notification to exist.

## What we give up
- **Event amplification is the main cost.** Each notification produced roughly 8 events. At the 23K/s target that is about 180K events/s, and the status topic was the first thing to fall behind in testing (about 706,000 messages of lag; coalescing saved about 42% of writes). This is the weakest part of the design.
- Eventual consistency: a status read can trail reality by the consumer lag.
- Webhooks that never arrive leave a notification at `SENT` forever, because nothing reconciles it yet.

## Evidence
Measured: rank guard, coalescing, legacy-item handling and phantom protection in integration tests; the lag and write numbers above in `docs/benchmarks.md`.

## Revisit when
Always, soon. Options in order of effort: drop per-channel `QUEUED` events, coalesce in the worker before publishing, batch writes, move the stream to an append-optimised store, and add a sweeper for notifications stuck at `SENT`.

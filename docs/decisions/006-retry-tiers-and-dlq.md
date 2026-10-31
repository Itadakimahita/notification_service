# 006. Non-blocking retry tiers and a dead-letter queue

**Status:** accepted

## Context
Providers fail transiently (timeouts, 5xx, 429). Retrying inside the consumer blocks its partition, so one failing message delays thousands behind it.

## Decision
On a transient failure the worker publishes the message to a retry topic for a delay tier (5 s, 30 s, 5 min, 30 min), stamps the due time in a header and moves on. A retry consumer per tier waits until due, then retries. After five attempts the message goes to `notifications.dlq.<channel>`, and `dlqctl` can inspect and replay it. Permanent errors (4xx, invalid token) are not retried.

## Why this is better
- **Partitions never stall.** The main lanes keep draining while failures wait elsewhere.
- **Failures are separated by how long they will take.** Short tiers recover blips quickly; long tiers do not hammer a provider that is down.
- **Nothing is silently dropped.** Exhausted messages are kept for inspection and replay. Replaying clears the attempt counter and the dedupe claim, so the message is sent for real.
- **Circuit breaker cooperation.** When a breaker is open the worker parks messages in the 30 s tier without burning an attempt.

## What we give up
- **Ordering is lost** for retried messages relative to later ones.
- **Delays are fixed.** There is no jitter yet; if many messages fail together they retry together in each tier. Adding jitter to the stamped due time is a small change.
- The DLQ depth metric counts retained messages, so replay does not reduce it; alert on growth.

## Evidence
Measured: with the provider failing 100%, a message walked all four tiers and landed in the DLQ at attempt 5, then `dlqctl replay` delivered it after recovery. With a 50% error provider plus 2 s latency, 500 of 500 sampled notifications were delivered.

## Revisit when
Provider outages routinely exceed the 30 minute tier (add a longer tier or a pause-by-provider switch), or ordering per user becomes a requirement.

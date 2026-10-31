# 009. Scheduler built on sharded time buckets

**Status:** accepted

## Context
Notifications can be delayed by `send_at` or held until a user's quiet hours end. Delays range from seconds to 90 days, and a million messages may be due at the same minute.

## Decision
The router writes delayed requests to a `schedule` table whose partition key is `<minute>#<shard>` (16 shards chosen by hashing the notification id) and whose sort key starts with the exact due time. Scheduler instances take shard leases, sweep every minute bucket from their checkpoint to the current minute each second, publish what is due back to `notifications.requested` and delete it. The request is re-marked as released, so it is not delayed twice.

## Why this is better
- **No hot partition.** A burst at 09:00 spreads over 16 shards instead of one key.
- **Catch-up is built in.** A stored checkpoint means a scheduler that was down for hours replays the missed minutes (tested with an item three hours overdue).
- **Safe against clock skew.** The scan always covers one extra minute behind the current one.
- **Quiet hours and `send_at` share one mechanism**, per channel, so one channel's quiet hours do not delay the others.
- **Alternatives are worse for this job.** DynamoDB TTL is not a timer (deletion can lag by many hours), a Redis sorted set loses schedules on restart, and Kafka cannot delay a single message.

## What we give up
- Up to a one second sweep delay on top of the due time.
- Two reads per shard per second even when idle (small, constant).
- The same duplicate window as other publishers: a crash between publishing and deleting re-emits an item. Deletion runs on a detached context to avoid this on graceful shutdown (found and fixed by the integration test).

## Evidence
Measured: integration test (due items emitted once, a three hour overdue item caught up, a future item untouched, repeated 10 times with no failures) and a live run where a `send_at` message arrived seconds after its due time and a marketing message waited out quiet hours.

## Revisit when
Sub-second precision is required, or the schedule table needs more than 16 shards for a single minute.

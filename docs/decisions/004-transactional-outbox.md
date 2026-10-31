# 004. Transactional outbox through DynamoDB Streams

**Status:** accepted

## Context
After the API stores a notification it must also publish it to Kafka. Doing both in the request is a dual write: if the process dies between them, the notification is stored but never sent, or sent but not stored.

## Decision
The API writes only to DynamoDB, atomically with its idempotency key (`TransactWriteItems`), and returns 202. A separate relay reads the table's Stream (inserts only), publishes to Kafka and records its position. Several relay instances share the stream shards through leases.

## Why this is better
- **No lost notifications.** Accepting means "durably stored". Publishing is retried by the relay until it succeeds.
- **The API does not depend on Kafka.** If Kafka is down the API keeps accepting.
- **No second outbox table.** The notification record is the outbox entry, so there is no extra write on the hot path.
- **Scales with shards.** Relay instances split the Stream shards, and parent shards finish before their children start.

## What we give up
- **An extra hop.** Accept-to-publish latency includes the Streams delay.
- **Duplicates are possible.** A crash after publishing but before the checkpoint republishes a batch. Consumers de-duplicate (see 007). Graceful shutdown writes its checkpoint on a detached context, which closes the window for planned restarts only.
- **Streams keep records for 24 hours.** If the relay is down longer, accepted notifications are lost. The alert `RelayHasNoShards` exists for this reason.

## Evidence
Measured: with a Kafka broker stopped, 2,000 of 2,000 requests were still accepted and delivered; the relay integration test proves no duplicates across a graceful restart; a real duplicate found by that test led to the checkpoint fix. The duplicate-on-crash window shows up as 4 extra provider sends in the worker `kill -9` run (that one is the worker's window, not the relay's).

## Revisit when
The Streams delay matters for latency, or Streams limits (24 hours, two consumers per shard) get in the way. A table-based outbox or a Kafka Connect source would be the alternatives.

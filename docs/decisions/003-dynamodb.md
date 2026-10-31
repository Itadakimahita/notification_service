# 003. DynamoDB for state, DynamoDB Local for development

**Status:** accepted (the choice of DynamoDB was made by the project owner; the local setup follows from it)

## Context
State has simple, known access patterns: get a notification by id, check an idempotency key, load one user's preferences and devices, list a user's inbox, find due scheduled items. Writes are heavy (every notification, every status change). Capacity planning for a spiky load is unwelcome.

## Decision
Store everything in DynamoDB on-demand. Run `amazon/dynamodb-local` in docker-compose so local and production execute the same code, including transactions and Streams. No Postgres at all.

## Why this is better
- **Writes scale without planning.** On-demand mode absorbs bursts with no shard or capacity management.
- **Features we use directly.** `TransactWriteItems` for idempotency, Streams for the outbox, TTL for retention, conditional writes for leases, rank guards and state transitions.
- **No dual implementation.** The first idea was Postgres locally and DynamoDB in production. The outbox works completely differently on each (relay on a table versus Streams), so bugs could appear only in production. One backend removes that class of bug.
- **One query for a user.** Profile, preferences and devices share a partition key, so the router loads them in a single query.

## What we give up
- **Access patterns must be designed first.** Ad hoc queries are not possible. Example: listing running campaigns uses a `Scan` with a filter, acceptable for tens of campaigns and wrong beyond that.
- **Lock-in to AWS.**
- **DynamoDB Local is not a performance model.** It is a single Java process. After about 140,000 notifications it slowed from seconds to a minute in our integration tests until restarted. Load numbers from it describe our services, not DynamoDB.

## Evidence
Measured: the idempotency transaction, rank guards and leases are covered by integration tests against DynamoDB Local; the Local slowdown is recorded in `docs/benchmarks.md`. **Not measured:** real DynamoDB throughput or cost.

## Revisit when
Queries beyond the key patterns are needed (reporting, search). Send events to an analytics store rather than querying DynamoDB.

# Design decisions

One file per significant choice. Each states the context, the decision, why it beats the alternatives, what it costs, and the evidence behind it. Evidence is labelled **measured** (we ran it and `docs/benchmarks.md` or the tests show it) or **reasoned** (argued from the workload, not benchmarked), so the claims can be checked and challenged.

| # | Decision | One-line reason |
|---|---|---|
| [001](001-go.md) | Go for all services | Cheap concurrency for network-bound work, tiny images |
| [002](002-kafka.md) | Kafka backbone | Replayable, partitioned log with visible backpressure |
| [003](003-dynamodb.md) | DynamoDB, DynamoDB Local for dev | Elastic writes, transactions and Streams, one code path |
| [004](004-transactional-outbox.md) | Outbox via DynamoDB Streams | No lost notifications, API independent of Kafka |
| [005](005-router-and-topic-layout.md) | Router stage, topic per channel and priority | Bulkheads and priority lanes |
| [006](006-retry-tiers-and-dlq.md) | Non-blocking retry tiers and DLQ | Failures never stall a partition |
| [007](007-idempotency-and-dedupe.md) | At-least-once with layered idempotency | Duplicates made harmless at every boundary |
| [008](008-redis-roles.md) | Redis for caches, caps, dedupe, limits | Fast shared state, shared provider quota |
| [009](009-scheduler.md) | Sharded time-bucket scheduler | No hot partition, catch-up after downtime |
| [010](010-status-as-events.md) | Status events with monotonic rank | Order-independent and replay-safe (but see its scaling concern) |
| [011](011-inapp-delivery.md) | Inbox plus sharded pub/sub WebSocket | Durable history, live push that scales per node |
| [012](012-campaigns.md) | Campaigns through the accept path | Reuses everything, resumable without double-sending |
| [013](013-protobuf.md) | Protobuf messages | Typed, compact, evolvable contract |
| [014](014-observability.md) | Prometheus, Grafana, traces through Kafka | Measures lag and tails, one trace per notification |
| [015](015-kubernetes.md) | Kustomize, KEDA on lag | Scale on the real backlog (not yet run on a cluster) |
| [016](016-policy-model.md) | Categories, preferences, quiet hours, caps | Critical messages protected, fatigue controlled |

## Where the design is weakest

Be sceptical of these first: the status event volume ([010](010-status-as-events.md)), the fail-closed dependency on Redis ([007](007-idempotency-and-dedupe.md), [008](008-redis-roles.md)), the 24 hour limit on Streams and idempotency records ([004](004-transactional-outbox.md)), and the isolation claim in [005](005-router-and-topic-layout.md) that has not been measured under a mixed load.

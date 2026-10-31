# 002. Kafka as the backbone

**Status:** accepted

## Context
The pipeline needs durable buffering between stages, per-user ordering where it matters, independent scaling of consumers, the ability to replay failed work, and survival of a broker loss.

## Decision
Use Kafka (KRaft, three brokers, replication factor 3, `min.insync.replicas` 2, producers with `acks=all`). Key every message by `user_id`. One topic per stage, and per channel and priority for dispatch.

## Why this is better
- **A log, not a queue.** Messages stay after consumption, so a DLQ can be replayed (`dlqctl replay`), a consumer group can be reset to skip a backlog, and a new consumer can start from the beginning. A classic queue deletes on ack, so replay needs a second copy.
- **Ordering and parallelism together.** Partitioning by `user_id` keeps one user's messages in order within a partition while 48 partitions allow 48 parallel consumers.
- **Backpressure is visible.** Consumer lag is a number we can alert on and scale on (KEDA), instead of queue length guesses.
- **Broker loss tolerated.** With three brokers and `min.insync.replicas` 2, losing one broker does not stop writes.

## What we give up
- Operational weight (brokers, partitions to plan). Partition count caps consumer parallelism and is hard to change later.
- No per-message TTL, priorities or delayed delivery. We build those from topics (priority lanes, retry tiers) and a scheduler table.
- Reading messages by hand needs tooling because the payload is binary.

## Evidence
Measured: with one of three brokers stopped for 15 seconds during load, all 2,000 requests were accepted and all 500 sampled notifications were delivered (`docs/benchmarks.md`). The relay and router moved about 124,000 queued events in roughly 20 seconds on a laptop. **Reasoned, not measured:** the comparison with RabbitMQ, SQS or NATS.

## Revisit when
Traffic is far lower than planned (a managed queue would be simpler) or the team cannot operate Kafka. A managed service (MSK) removes most of the operational cost without changing the design.

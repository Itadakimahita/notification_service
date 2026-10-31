# Notification Platform — Design Plan

> This is the design written before implementation. See the [README](../README.md) for what was built and [benchmarks](benchmarks.md) for measured results. Differences: the API listens on 8081, status events are produced by the router and workers and applied by the status service, and the Kubernetes manifests use Kustomize and KEDA instead of Helm.

## Context
The repo (`/Users/dika/Documents/highload/notification_service`) is empty (only `CLAUDE.md`). We are building a portfolio-grade, high-load notification platform from the user's whiteboard design (push / email / SMS / in-app, 200M/day, 23K/s peak, p95 enqueue < 100 ms, at-least-once + idempotency).

**Decided stack:** Go · Kafka (KRaft) · DynamoDB (prod) / DynamoDB Local (compose, same code path) · Redis · Protobuf events · Prometheus/Grafana/OTel · k6.
Target: runs fully in docker-compose with mock providers, load-tested to 23K/s, k8s manifests as a final phase.

Project rules (CLAUDE.md): no Claude contributor/co-author lines in commits; minimal comments (one line per function describing what it does); proactively suggest improvements.

---

## 1. What changes vs. the original design (and why)

| # | Gap in original | Improvement | Industry reference |
|---|---|---|---|
| 1 | Workers do everything (prefs, templates, sending) | Add a **Router** stage: resolve prefs/opt-out/quiet hours/devices/template → fan out to **per-channel topics**. Workers only send. | LinkedIn ATC (Air Traffic Controller) sits between producers and channels |
| 2 | One queue for all channels | **Bulkhead per channel × priority** (`dispatch.push.high`, `dispatch.sms.low` …). Slow SMS provider can't block push; marketing can't starve OTPs. | Netflix RENO: priority-based queues |
| 3 | Retries block the consumer | **Non-blocking retry topics** (5s → 30s → 5m → 30m) then DLQ; partitions keep flowing. | Uber "Reliable Reprocessing & DLQ with Kafka" |
| 4 | Scheduler floating, unconnected | Scheduler owns **send_at** *and* **quiet-hours deferral**; sharded time buckets in DynamoDB. | — |
| 5 | Outbox ambiguous for DynamoDB | **CDC outbox**: `TransactWriteItems(notification + idempotency key)` → **DynamoDB Streams** → Relay → Kafka. No dual write. | Standard transactional-outbox via CDC |
| 6 | Preferences only per channel | Preferences per **(category, channel)**; categories `transactional` (bypass quiet hours/marketing opt-out) vs `marketing`. | Slack/LinkedIn preference models |
| 7 | No freshness | `expires_at` per notification — never deliver a stale OTP. | — |
| 8 | No fatigue control | **Frequency caps** per user/category (Redis counters). | LinkedIn ATC |
| 9 | In-app has no transport | **Inbox table + WebSocket gateway** (Redis pub/sub to the node holding the socket). | — |
| 10 | Every attempt written to DB | Hot path stores only **latest status per channel**; attempts/events go to Kafka `status` topic → batched updates + analytics (ClickHouse, optional). Cuts DynamoDB WCU ~3×. | — |
| 11 | Bulk campaigns go through the same API | **Campaign service** streams audience in chunks at a throttled rate into the low-priority lane. | — |
| 12 | Token hygiene missing | Provider "unregistered/invalid token" → **deactivate DeviceToken** automatically. | FCM/APNs best practice |

---

## 2. Capacity math (drives partition/replica counts)
- 200M/day → **2.3K/s avg, 23K/s peak** requests; avg ~1.5 channels each → **~35K/s dispatch msgs at peak**.
- Msg ~1 KB → ~35 MB/s Kafka ingress (×3 replication).
- Little's law for workers: provider latency ~100 ms → 35K/s × 0.1 s = **~3.5K in-flight sends** → e.g. 24 worker pods × ~150 goroutines.
- Partitions: `requested` 48, each `dispatch.*.high` 48, `*.low` 24 (allows scale-out to that many consumers).
- Storage: ~200 GB/day notification data → DynamoDB TTL 30 days, archive via export to S3.

---

## 3. Architecture

```
Producers ──► API Gateway/LB ──► [api]  validate, auth(api-key), idempotency
                                   │ TransactWriteItems(notification + idem key)  ──► DynamoDB
                                   ▼                                                    │ Streams
                                 202 Accepted                                     [relay] (lease-based leader)
                                                                                        ▼
                                                                        kafka: notifications.requested (key=user_id)
                                                                                        ▼
  [scheduler] ◄── send_at / quiet hours ── [router] prefs(Redis cache) · opt-out · freq cap · expiry
       │ due → back to requested                │ template render (locale fallback) · device resolve
                                                ▼
                     dispatch.{push,email,sms,inapp}.{high,low}  (key=user_id)
                                                ▼
                 [worker --channel=X]  token-bucket per provider · circuit breaker · dedupe
                     │ transient fail → retry.{X}.{5s,30s,5m,30m} → dlq.{X}
                     ▼                                                       │ [dlqctl] replay
            Adapters: FCM/APNs | SMTP/SES | SMS | in-app(inbox + WS gateway)
                     ▼
            notifications.status ◄── [status] provider webhooks
                     ▼
            [status] batched status updates → DynamoDB ; (optional) → ClickHouse analytics
```

### Services (`cmd/`)
| Service | Responsibility |
|---|---|
| `api` | REST API, idempotency, persist, 202. Also prefs/devices/templates CRUD, status read, inbox read |
| `relay` | DynamoDB Streams → Kafka, checkpoints + leader lease in DynamoDB |
| `router` | Policy + fan-out (prefs, quiet hours, caps, expiry, render, devices) |
| `scheduler` | Polls sharded due-buckets, re-emits due notifications |
| `worker` | One binary, `--channel` flag; consumes high+low lanes (separate consumer groups/pools), retry tiers |
| `status` | Webhook receiver + status consumer, batched DynamoDB updates |
| `inapp` | WebSocket gateway; Redis pub/sub fan-in |
| `campaign` | Bulk audience (CSV from MinIO/S3) → throttled requested events |
| `mockprovider` | Fake FCM/APNs/SMS with configurable latency/error rate; sends delivery webhooks |
| `dlqctl` | CLI: inspect/replay DLQ by filter |
| `migrate` | Create DynamoDB tables + Kafka topics |

### Status state machine
`ACCEPTED → ROUTED → (SCHEDULED) →` per channel `QUEUED → SENT → DELIVERED | FAILED | SUPPRESSED | EXPIRED`

---

## 4. Data model (DynamoDB, on-demand)
| Table | PK | SK | Notes |
|---|---|---|---|
| `notifications` | `notification_id` (UUIDv7) | — | payload, priority, category, per-channel status map, TTL; **Stream NEW_IMAGE** = outbox |
| `idempotency` | `producer#idem_key` | — | conditional put `attribute_not_exists`, TTL 24h, stores notification_id for replay response |
| `users` | `user_id` | `PROFILE` / `PREF#category#channel` / `DEVICE#token_hash` | one query loads everything router needs; cached in Redis 5 min |
| `templates` | `template_id` | `channel#locale#version` | locale fallback `kk-KZ → kk → en` |
| `schedule` | `bucket_minute#shard(0..15)` | `due_ts#notification_id` | sharded to avoid hot partitions |
| `inbox` | `user_id` | `ts#notification_id` | in-app history, read flag, TTL |
| `leases` | `name` | — | relay/scheduler shard ownership + stream checkpoints |

Redis: preference cache, frequency-cap counters, provider token buckets, worker dedupe (`SETNX sent:{nid}:{channel}:{target}` TTL 24h), in-app pub/sub.

---

## 5. Reliability rules
- **At-least-once** everywhere; commit offsets only after handling (franz-go manual commit, batch).
- **Dedupe** at API (idempotency table) and at worker (Redis SETNX before send + status check). Residual dup window (crash between provider send and mark) documented; use provider dedupe keys where supported (APNs `apns-collapse-id`, email `Message-ID`).
- **Error classification**: transient (timeout, 5xx, 429) → retry tier; permanent (4xx, invalid token) → FAILED (+ token deactivation), no retry.
- **Backoff with jitter** via retry tiers; max 5 attempts → DLQ. Retry consumer pauses the partition until head message is due.
- **Circuit breaker** per provider (sony/gobreaker); open → route to retry tier, not hammer.
- **Rate limits**: per-provider token bucket in Redis, with capacity reserved for the high lane (e.g. 70/30); per-producer quotas at API.
- **Expiry**: router and worker drop messages past `expires_at` → EXPIRED.
- **Graceful shutdown**: stop fetching, drain in-flight, commit, exit.

---

## 6. API (public, JSON)
- `POST /v1/notifications` — headers `Authorization: ApiKey …`, `Idempotency-Key`; body `{user_id, template_id, category, priority, channels?, data, send_at?, expires_at?}` → `202 {notification_id}`
- `POST /v1/notifications/batch` (≤1000 items, BatchWrite-style)
- `GET /v1/notifications/{id}` → per-channel status
- `PUT /v1/users/{id}/preferences`, `POST/DELETE /v1/users/{id}/devices`
- `GET /v1/users/{id}/inbox?cursor=`, `POST /v1/users/{id}/inbox/{nid}/read`
- `POST /v1/templates`, `POST /v1/campaigns`
- `POST /v1/webhooks/{provider}` (on `status` service)
- `GET /healthz`, `/readyz`, `/metrics` on every service

Internal Kafka events: Protobuf (`proto/notification/v1/*.proto`, generated with `buf`).

---

## 7. Stack & libraries
- Go 1.23+, `net/http` + `go-chi/chi`, `log/slog`, `caarlos0/env` config
- Kafka: **`twmb/franz-go`** (pure Go, fastest; avoids cgo of confluent-kafka-go)
- DynamoDB: `aws-sdk-go-v2` (`dynamodb`, `dynamodbstreams`)
- Redis: `redis/go-redis/v9`
- Resilience: `sony/gobreaker`, Redis-Lua token bucket
- Observability: `prometheus/client_golang`, OpenTelemetry → Jaeger
- Tests: `testcontainers-go` (DynamoDB Local, Kafka, Redis), `stretchr/testify`
- Load: **k6**; lint: `golangci-lint`; CI: GitHub Actions
- Local infra (compose): `apache/kafka` (KRaft, 3 brokers), `kafka-ui`, `amazon/dynamodb-local` (Streams on), `dynamodb-admin`, `redis`, `mailpit` (real SMTP sink for email channel), `minio`, `prometheus`, `grafana`, `jaeger`, optional `clickhouse`
- Prod (documented, later): EKS, MSK, DynamoDB on-demand, ElastiCache, SES/FCM/APNs/SMS provider, KEDA autoscaling on consumer lag

---

## 8. Repo layout
```
cmd/{api,relay,router,scheduler,worker,status,inapp,campaign,mockprovider,dlqctl,migrate}/main.go
internal/
  domain/          entities, enums, state machine
  store/dynamo/    repositories (notifications, idempotency, users, templates, schedule, inbox, leases)
  kafkax/          producer/consumer wrappers, retry-tier routing
  events/          generated protobuf
  router/          policy pipeline (prefs, quiet hours, caps, expiry, render)
  channels/        Sender interface + push/email/sms/inapp adapters
  ratelimit/  breaker/  dedupe/  templates/
  httpx/  config/  observability/
proto/notification/v1/
deploy/compose/  deploy/k8s/  deploy/grafana/  deploy/prometheus/
loadtest/k6/
docs/architecture.md   (this design + diagrams)
Makefile  go.mod  .golangci.yml  .github/workflows/ci.yml
```

---

## 9. Implementation phases
Each phase ends runnable via `make up` with tests passing.

**Phase 0 — Skeleton & infra**
`go.mod`, Makefile (`up/down/migrate/test/lint/loadtest/proto`), docker-compose with all infra, `cmd/migrate` (tables + topics), shared `config`, `observability` (slog, metrics, tracing), CI, `docs/architecture.md`.

**Phase 1 — Accept path (API)**
Domain types, Dynamo repos, `POST /v1/notifications` with `TransactWriteItems` + idempotency (replay returns same id), validation, API-key auth, per-producer rate limit, batch endpoint, status GET. Integration tests on DynamoDB Local. Goal: p95 < 100 ms locally.

**Phase 2 — Relay (outbox CDC)**
Streams shard discovery, leased leader, checkpointing, publish protobuf to `notifications.requested` (key=user_id, idempotent producer, acks=all).

**Phase 3 — Router**
Users/prefs/devices CRUD on API + Redis cache with invalidation; policy pipeline (opt-out, category rules, quiet hours w/ timezone → scheduler, freq caps, expiry), template engine with locale fallback, fan-out to `dispatch.{channel}.{priority}`.

**Phase 4 — Workers + mock providers**
`Sender` interface, adapters (email → Mailpit SMTP, push/SMS → `mockprovider`), concurrency pool per partition, token bucket, circuit breaker, dedupe, error classification, retry tiers, DLQ, `dlqctl` replay, token deactivation.

**Phase 5 — Scheduler**
`send_at` and quiet-hours deferral into sharded `schedule` table; shard leases; re-emit due items; catch-up after downtime.

**Phase 6 — Status & in-app**
Webhook ingestion, `notifications.status` consumer with batched updates; inbox table, `inapp` WebSocket gateway with Redis pub/sub; inbox API.

**Phase 7 — Campaigns**
Upload audience CSV to MinIO, chunked streaming, throttled emission into low lane, progress tracking, pause/cancel.

**Phase 8 — Observability**
RED metrics per service, consumer lag exporter, DLQ depth, provider error rates, Grafana dashboards, alert rules, end-to-end traces (trace context in Kafka headers).

**Phase 9 — Load & chaos testing**
k6 scenarios: 2.3K/s sustained 30 min, 23K/s burst 5 min, campaign + transactional mix (verify OTP latency unaffected). Chaos: kill workers, provider 50% errors / 2 s latency, stop a Kafka broker, Redis restart. Record results in `docs/benchmarks.md`.

**Phase 10 — Kubernetes (optional)**
kind cluster, Helm chart per service, HPA for API, KEDA on Kafka lag for workers/router, PodDisruptionBudgets.

---

## 10. Verification
- `make up && make migrate` → all containers healthy; kafka-ui shows topics; dynamodb-admin shows tables.
- `curl -X POST localhost:8080/v1/notifications -H 'Idempotency-Key: k1' …` twice → same `notification_id`, single delivery.
- Email channel → message visible in Mailpit UI (`localhost:8025`); push/SMS → mockprovider logs + DELIVERED via webhook; in-app → received on a `websocat` connection.
- Set mockprovider error rate 30% → retries in retry topics, eventual DELIVERED; 100% → DLQ, `dlqctl replay` recovers.
- Quiet hours / `send_at` → status SCHEDULED, delivered at due time.
- `make test` (unit + testcontainers integration), `make lint`.
- `make loadtest` → Grafana shows API p95 < 100 ms, lag drains, no message loss (k6 sent count == DELIVERED+FAILED+SUPPRESSED count).
- Caveat: DynamoDB Local doesn't model throttling/WCU; load results reflect our services, not DynamoDB capacity.

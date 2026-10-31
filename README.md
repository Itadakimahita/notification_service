# Notification Platform

A high-load notification service: push, email, SMS and in-app messages, sent immediately or scheduled, with user preferences, templates, retries, a dead-letter queue, bulk campaigns and delivery tracking. Written in Go on Kafka, DynamoDB and Redis.

Design target: 100M daily active users, 200M notifications per day (2.3K/s average, 23K/s peak), enqueue p95 under 100 ms, durable acceptance and at-least-once delivery.

| Read | For |
|---|---|
| This file | How it works, with diagrams. How to run it |
| [docs/decisions/](docs/decisions/README.md) | One file per design choice: why it was chosen and why it is better |
| [docs/observability.md](docs/observability.md) | Metrics catalogue, the Grafana dashboard, p50/p95/p99, alerts, traces |
| [docs/benchmarks.md](docs/benchmarks.md) | Measured results and what was not measured |
| [docs/kubernetes.md](docs/kubernetes.md) | Cluster manifests and scaling |
| [docs/architecture.md](docs/architecture.md) | The original design plan |

The diagrams below use Mermaid and render on GitHub and in most editors.

---

## 1. The system at a glance

Every arrow between boxes is either an HTTP call, a Kafka topic (rounded boxes) or a DynamoDB table (cylinders). Services never call each other directly except to providers.

```mermaid
flowchart LR
    P["Producers<br/>(apps, backends)"] -->|"POST /v1/notifications"| API[api]

    subgraph accept ["Accept path: durable, fast"]
        API -->|"one transaction:<br/>notification + idempotency key"| DDB[("DynamoDB")]
    end

    DDB -.->|"Streams"| RELAY[relay]
    RELAY --> REQ(["notifications.requested"])

    REQ --> ROUTER[router]
    ROUTER <-->|"preferences, caps"| REDIS[("Redis")]
    ROUTER -->|"delayed or quiet hours"| SCHED[("schedule table")]
    SCHED --> SCHEDULER[scheduler]
    SCHEDULER --> REQ

    ROUTER --> D1(["dispatch.push.high / low"])
    ROUTER --> D2(["dispatch.email.high / low"])
    ROUTER --> D3(["dispatch.sms.high / low"])
    ROUTER --> D4(["dispatch.inapp.high / low"])

    D1 --> W1[worker push]
    D2 --> W2[worker email]
    D3 --> W3[worker sms]
    D4 --> W4[worker inapp]

    W1 --> FCM["FCM / APNs"]
    W2 --> SMTP["SMTP / SES"]
    W3 --> SMS["SMS provider"]
    W4 --> INBOX[("inbox table")]
    W4 --> REDIS

    W1 -. "failure" .-> RETRY(["retry tiers"])
    RETRY --> W1
    RETRY -. "exhausted" .-> DLQ(["DLQ"])

    FCM -. "webhook" .-> STATUS[status]
    W1 & W2 & W3 & W4 & ROUTER --> ST(["notifications.status"])
    ST --> STATUS
    STATUS --> DDB

    REDIS --> GW["inapp gateway<br/>(WebSocket)"]
    GW --> USER["User device"]

    CAMP[campaign runner] -->|"same accept path"| DDB
    S3[("S3 audience CSV")] --> CAMP
```

**Why it is shaped this way:** acceptance only needs DynamoDB, so the API stays fast and keeps working when Kafka or a provider is down. Everything after that is asynchronous and each stage can scale, fail and recover on its own. Details: [004 outbox](docs/decisions/004-transactional-outbox.md), [005 router and topics](docs/decisions/005-router-and-topic-layout.md).

---

## 2. Life of one notification

```mermaid
sequenceDiagram
    autonumber
    participant Prod as Producer
    participant API as api
    participant DB as DynamoDB
    participant Rel as relay
    participant K as Kafka
    participant R as router
    participant W as worker
    participant Prov as Provider
    participant St as status service

    Prod->>API: POST /v1/notifications (Idempotency-Key)
    API->>API: validate, authenticate, rate limit
    API->>DB: transaction: idempotency key + notification
    DB-->>API: committed
    API-->>Prod: 202 Accepted (notification_id)
    Note over API,Prod: Done in milliseconds. Nothing below is on the request path.

    DB-->>Rel: Streams: new notification
    Rel->>K: publish to notifications.requested
    Rel->>DB: save checkpoint

    K->>R: consume request
    R->>R: expiry, opt-out, quiet hours, caps
    R->>R: render template, pick devices or address
    R->>K: publish to dispatch.channel.priority
    R->>K: status ROUTED and QUEUED

    K->>W: consume dispatch message
    W->>W: claim in Redis, take rate-limit token
    W->>Prov: send
    Prov-->>W: accepted
    W->>K: status SENT

    Prov-->>St: webhook delivered
    St->>K: status DELIVERED
    K->>St: consume status events
    St->>DB: update only if rank is higher
```

A producer sees only steps 1 to 6. A notification's current state is read with `GET /v1/notifications/{id}`, which shows overall and per-channel status.

---

## 3. The transactional outbox (no lost notifications)

Publishing to Kafka from the API would be a *dual write*: a crash between the database write and the publish loses or orphans a notification. Instead the API writes once, and a relay reads the database's own change stream.

```mermaid
sequenceDiagram
    participant API as api
    participant DB as DynamoDB table
    participant S as DynamoDB Stream
    participant L as lease table
    participant R1 as relay A
    participant R2 as relay B
    participant K as Kafka

    API->>DB: put notification (atomic with idempotency key)
    DB->>S: insert record
    R1->>L: acquire lease for shard 1
    R2->>L: acquire lease for shard 2
    R1->>S: read shard 1 from checkpoint
    R1->>K: publish batch (acks from all replicas)
    K-->>R1: acknowledged
    R1->>L: save checkpoint and renew lease
    Note over R1,L: Crash before this line means the batch is republished.<br/>Consumers de-duplicate, so that is safe.
    R1--xL: relay A dies, lease expires
    R2->>L: take over shard 1 from last checkpoint
```

Several relays share the work by shard. Parent shards are finished before their children start. See [004](docs/decisions/004-transactional-outbox.md).

---

## 4. What the router decides

The router applies policy once so workers can stay simple. Each channel is decided independently.

```mermaid
flowchart TD
    A([Request from Kafka]) --> B{"Expired?"}
    B -->|yes| B1["status EXPIRED, stop"]
    B -->|no| C{"send_at in the future<br/>and not already released?"}
    C -->|yes| C1["store in schedule table<br/>status SCHEDULED, stop"]
    C -->|no| D{"User known?"}
    D -->|no| D1["status SUPPRESSED<br/>unknown user, stop"]
    D -->|yes| E["For each requested channel<br/>(default: push, inapp, email)"]
    E --> F{"Opted out<br/>for this category?"}
    F -->|yes| F1["SUPPRESSED: opted out"]
    F -->|no| G{"Marketing and inside<br/>quiet hours?"}
    G -->|yes| G1["defer this channel to end of window<br/>SCHEDULED"]
    G -->|no| H{"Has a device, email<br/>or phone number?"}
    H -->|no| H1["SUPPRESSED: no target"]
    H -->|yes| I["Channel is ready"]
    I --> J{"Marketing and daily<br/>cap reached?"}
    J -->|yes| J1["SUPPRESSED: frequency cap"]
    J -->|no| K{"Template found<br/>and renders?"}
    K -->|no| K1["FAILED with reason"]
    K -->|yes| L["Publish to dispatch.channel.priority<br/>status QUEUED"]
```

Transactional messages (codes, receipts) skip the quiet-hours and cap checks and use the high-priority lane. See [016](docs/decisions/016-policy-model.md).

---

## 5. What a worker does, and how failures are handled

```mermaid
flowchart TD
    A([Dispatch message]) --> B{"Expired?"}
    B -->|yes| B1["status EXPIRED"]
    B -->|no| C["Claim send slot in Redis"]
    C -->|"already sent"| C1["skip (duplicate)"]
    C -->|"another worker mid-send"| C2["retry in 5 s"]
    C -->|claimed| D["Wait for rate-limit token<br/>(separate high and low shares)"]
    D -->|"timed out"| D1["release claim, retry in 5 s"]
    D --> E{"Circuit breaker open?"}
    E -->|yes| E1["release claim, park in 30 s tier<br/>(no attempt used)"]
    E -->|no| F["Call provider"]
    F -->|success| G["mark sent, status SENT"]
    F -->|"token invalid"| H["status FAILED, deactivate device"]
    F -->|"permanent 4xx"| I["status FAILED, no retry"]
    F -->|"transient 5xx / 429 / timeout"| J{"Attempts left?"}
    J -->|yes| J1["release claim, publish to retry tier"]
    J -->|no| J2["publish to DLQ, status FAILED"]
```

The retry tiers are separate topics, so a failing message never blocks the partition it came from:

```mermaid
flowchart LR
    M(["main lane"]) -->|"fail 1"| T1(["retry 5s"])
    T1 -->|"fail 2"| T2(["retry 30s"])
    T2 -->|"fail 3"| T3(["retry 5m"])
    T3 -->|"fail 4"| T4(["retry 30m"])
    T4 -->|"fail 5"| DLQ(["DLQ"])
    T1 -->|ok| OK(["sent"])
    T2 -->|ok| OK
    T3 -->|ok| OK
    T4 -->|ok| OK
    M -->|ok| OK
    DLQ -->|"dlqctl replay"| M
```

See [006](docs/decisions/006-retry-tiers-and-dlq.md) and [007](docs/decisions/007-idempotency-and-dedupe.md).

---

## 6. Status of a notification

Each channel has its own status. A status only moves to a higher rank, so late or duplicate events cannot move it backwards.

```mermaid
stateDiagram-v2
    [*] --> ACCEPTED: API stored it
    ACCEPTED --> SCHEDULED: send_at or quiet hours
    SCHEDULED --> QUEUED: released by scheduler
    ACCEPTED --> QUEUED: router published
    ACCEPTED --> SUPPRESSED: opt-out, cap, no target
    ACCEPTED --> EXPIRED: expires_at passed
    QUEUED --> SENT: provider accepted
    QUEUED --> FAILED: permanent error or retries exhausted
    QUEUED --> EXPIRED: expired before send
    SENT --> DELIVERED: provider webhook
    FAILED --> SENT: replayed from DLQ
    DELIVERED --> [*]
    SUPPRESSED --> [*]
    EXPIRED --> [*]
```

`SENT` is final for email, because SMTP gives no delivery receipt. In-app is marked `DELIVERED` when stored. See [010](docs/decisions/010-status-as-events.md).

---

## 7. Scheduling

Delayed notifications are stored by due time in sharded one-minute buckets, so a burst at 09:00 spreads over many partitions instead of one.

```mermaid
flowchart LR
    R[router] -->|"due time, notification id"| T[("schedule table<br/>key: minute#shard<br/>sort: exact due time")]
    subgraph shards ["16 shards per minute"]
        T --> S0["bucket 12:00 shard 0"]
        T --> S1["bucket 12:00 shard 1"]
        T --> SN["... shard 15"]
    end
    SCH1["scheduler A<br/>(owns shards by lease)"] -->|"every second:<br/>checkpoint to now"| shards
    SCH2["scheduler B"] --> shards
    shards -->|"due items"| K(["notifications.requested<br/>marked released"])
    SCH1 -->|"then delete"| shards
```

After downtime a scheduler resumes from its saved checkpoint and replays every missed minute. See [009](docs/decisions/009-scheduler.md).

---

## 8. Bulk campaigns

```mermaid
sequenceDiagram
    participant P as Producer
    participant API as api
    participant S3 as Object storage
    participant C as campaign runner
    participant DB as DynamoDB
    participant Rel as relay and pipeline

    P->>API: POST /v1/campaigns
    API-->>P: campaign id and presigned upload URL
    P->>S3: PUT audience.csv (direct, not through the API)
    P->>API: POST /v1/campaigns/{id}/start
    loop every chunk (about half a second of work)
        C->>S3: read next lines from saved byte offset
        C->>DB: create one notification per row<br/>(idempotency key = user id)
        C->>DB: save offset and counters
        C->>DB: re-read status (pause or cancel?)
    end
    DB-->>Rel: normal outbox path, low-priority lane
    Note over C,DB: A crash replays at most one chunk.<br/>Idempotency keys turn the replay into no-ops.
```

See [012](docs/decisions/012-campaigns.md).

---

## 9. In-app messages and live push

```mermaid
sequenceDiagram
    participant W as worker inapp
    participant DB as inbox table
    participant R as Redis
    participant G1 as gateway node 1
    participant G2 as gateway node 2
    participant U as User device

    U->>G1: WebSocket connect (user_id)
    G1->>R: subscribe to the shard of this user
    Note over G1,R: 64 shards. A node subscribes only to<br/>shards that have a connected user.
    W->>DB: store message (durable history)
    W->>R: publish to the user's shard channel
    R-->>G1: message
    G1-->>U: push over the socket
    R-->>G2: nothing (not subscribed to that shard)
    U->>DB: GET inbox after reconnecting
    Note over U,DB: Pub/sub is fire and forget, so clients fetch the inbox on reconnect.
```

See [011](docs/decisions/011-inapp-delivery.md).

---

## 10. Data model

DynamoDB tables (on-demand). Tables for users hold several kinds of item under one partition key so the router loads a user in a single query.

```mermaid
erDiagram
    NOTIFICATIONS {
        string partition_key "notification id (UUIDv7)"
        string user_id
        string template_id
        string category "transactional or marketing"
        string priority "high or low"
        map channel_status "per channel latest status"
        map channel_rank "guards against going backwards"
        string traceparent "trace context for the outbox hop"
        number ttl "30 day retention"
    }
    IDEMPOTENCY {
        string partition_key "producer#key"
        string notification_id
        number ttl "24 hours"
    }
    USERS {
        string partition_key "user id"
        string sort_key "PROFILE or PREF#category#channel or DEVICE#id"
    }
    TEMPLATES {
        string partition_key "template id"
        string sort_key "channel#locale#version"
    }
    SCHEDULE {
        string partition_key "minute#shard"
        string sort_key "due time#notification#channels"
    }
    INBOX {
        string partition_key "user id"
        string sort_key "created_at_notification"
        bool read
    }
    LEASES {
        string partition_key "relay#shard, scheduler#n, campaign#id"
        string owner
        string checkpoint
    }
    CAMPAIGNS {
        string partition_key "campaign id"
        string status "DRAFT RUNNING PAUSED COMPLETED CANCELLED FAILED"
        number offset "resume position in the CSV"
    }
    IDEMPOTENCY ||--|| NOTIFICATIONS : "written in one transaction"
    USERS ||--o{ NOTIFICATIONS : "receives"
    TEMPLATES ||--o{ NOTIFICATIONS : "rendered for"
    NOTIFICATIONS ||--o{ SCHEDULE : "delayed copies"
    USERS ||--o{ INBOX : "in-app history"
    CAMPAIGNS ||--o{ NOTIFICATIONS : "creates"
```

---

## 11. Kafka topics

Messages are keyed by `user_id`, so one user's messages stay in order within a partition. Counts are partitions.

```mermaid
flowchart TB
    subgraph core ["Pipeline"]
        REQ(["notifications.requested (48)"])
        ST(["notifications.status (24)"])
    end
    subgraph lanes ["Dispatch lanes, one pair per channel"]
        H(["dispatch.channel.high (48)"])
        L(["dispatch.channel.low (24)"])
    end
    subgraph fail ["Failure handling, per channel"]
        RT(["retry.channel.5s / 30s / 5m / 30m (12 each)"])
        DLQ(["dlq.channel (6)"])
        RDLQ(["dlq.router (6)"])
    end
    REQ --> H
    REQ --> L
    H --> RT
    L --> RT
    RT --> DLQ
    REQ -. "undecodable" .-> RDLQ
    H -. "status events" .-> ST
```

Channels are push, email, sms and inapp, which gives 31 topics in total. Consumers can scale up to the partition count and no further. See [002](docs/decisions/002-kafka.md).

---

## 12. Observability

```mermaid
flowchart LR
    subgraph services ["Every service"]
        M["/metrics, /healthz, /readyz"]
        T["traces (OTLP)"]
    end
    K(["Kafka"]) --> LE["lag exporter<br/>consumer lag, DLQ depth"]
    M --> PROM[Prometheus]
    LE --> PROM
    PROM --> GRAF["Grafana dashboard<br/>44 panels, p50 p95 p99"]
    PROM --> ALERT["Alert rules"]
    T --> JAEGER[Jaeger]
    JAEGER --> GRAF
```

One notification is one trace, carried across the DynamoDB Streams hop and every Kafka hop:

```mermaid
flowchart LR
    A["api<br/>notification.accept"] -->|"traceparent stored<br/>on the record"| REL["relay<br/>copies to Kafka header"]
    REL --> RT["router<br/>router.route"]
    RT -->|"header re-injected"| WK["worker<br/>worker.send"]
```

Everything about metrics and alerts is in [docs/observability.md](docs/observability.md).

---

## 13. Scaling and deployment

```mermaid
flowchart TB
    LB["Load balancer"] --> API["api<br/>HPA on CPU, 3 to 50"]
    subgraph kafka_scaled ["KEDA scales on consumer lag, up to the partition count"]
        RTR["router<br/>3 to 48"]
        WRK["workers per channel<br/>2 to 48"]
        STS["status<br/>3 to 24"]
    end
    subgraph leased ["Fixed replicas, coordinate through DynamoDB leases"]
        REL2["relay x2"]
        SCH["scheduler x2"]
        CMP["campaign x2"]
    end
    API --> DDB[("DynamoDB")]
    REL2 --> DDB
    kafka_scaled --> KFK(["Kafka (MSK)"])
    leased --> KFK
    kafka_scaled --> RED[("Redis (ElastiCache)")]
```

The Kubernetes manifests render and pass consistency tests but have not been applied to a cluster. See [docs/kubernetes.md](docs/kubernetes.md) and [015](docs/decisions/015-kubernetes.md).

---

## Run it locally

Requires Go 1.26 and Docker.

```sh
make up        # Kafka (3 brokers), DynamoDB Local, Redis, Mailpit, S3Mock, Prometheus, Grafana, Jaeger
make migrate   # tables, topics, bucket
make run       # builds and starts all 13 processes, logs in .run/
curl -XPOST localhost:8081/v1/notifications -H 'Authorization: ApiKey demo-key' \
  -H 'Idempotency-Key: k1' -d '{"user_id":"u1","template_id":"welcome","category":"transactional","data":{"name":"Dias"}}'
make stop
make test      # unit and integration tests (integration tests skip without the compose stack)
```

| Service | Ports (service / metrics) |
|---|---|
| api | 8081 / 9101 |
| relay, router, scheduler | - / 9102, 9103, 9106 |
| status (webhooks) | 8082 / 9107 |
| inapp (WebSocket) | 8083 / 9108 |
| workers push, email, inapp, sms | - / 9104, 9114, 9124, 9134 |
| campaign, lagexporter, mockprovider | - / 9109, 9110, 9105 (provider 9200) |

Tools: Grafana :3000, Prometheus :9090, Jaeger :16686, Mailpit :8025, Kafka UI :8090, DynamoDB admin :8001. Host ports 6380 (Redis) and 9300 (S3) are shifted from the defaults to avoid clashing with other local software.

Other commands: `make loadtest`, `make chaos`, `make images`, `make k8s-test`, `python3 scripts/gen_dashboard.py`, `python3 scripts/check_dashboard.py`.

## API

All routes need `Authorization: ApiKey <key>`.

| Method and path | Purpose |
|---|---|
| `POST /v1/notifications` (`Idempotency-Key` header) | Accept one notification, returns 202 |
| `POST /v1/notifications/batch` | Up to 1000, per-item results |
| `GET /v1/notifications/{id}` | Overall and per-channel status |
| `PUT /v1/users/{id}`, `PUT /v1/users/{id}/preferences` | Profile, per category and channel preferences with quiet hours |
| `POST /v1/users/{id}/devices`, `DELETE .../devices/{device_id}` | Push tokens |
| `POST /v1/templates` | New template version (per channel and locale) |
| `GET /v1/users/{id}/inbox`, `POST .../inbox/{item}/read` | In-app inbox |
| `POST /v1/campaigns`, `GET/POST /v1/campaigns/{id}[/start,pause,resume,cancel]` | Bulk campaigns, audience CSV uploaded to a presigned URL |
| `POST /v1/webhooks/{provider}` (status service, `X-Webhook-Key`) | Provider delivery callbacks |
| `GET /v1/ws?user_id=` (inapp service) | Live in-app messages |

## Not done, and what I would improve

Real providers are not implemented: push, SMS and email go to the mock provider and Mailpit. PII and device tokens are stored unencrypted. Kubernetes manifests were never applied to a cluster. The 23K/s peak, a 30 minute soak, a Redis restart and a campaign plus transactional mix were not tested (see the benchmark report for what was).

Recommended next steps, most valuable first:

1. **Cut status event amplification.** Each notification emits about 8 status events; at 23K/s that is roughly 180K events/s into DynamoDB, and it was the first thing to fall behind in testing. Drop the per-channel `QUEUED` event, coalesce in the worker, batch writes, or move the event stream to a store built for appends.
2. **Remove the batch barrier in consumers.** A batch waits for its slowest message before the next poll. Pipelining per partition would raise worker throughput without more pods.
3. **Reconcile notifications stuck at `SENT`.** If a provider callback never arrives, nothing ever marks the notification final. Add a sweeper that moves old `SENT` records to an explicit unknown state and alerts on the count.
4. **Put user-scoped auth in front of inbox and WebSocket.** They currently take a producer API key, which is fine server-to-server but must not reach mobile clients; issue short-lived per-user tokens.
5. **Decide the failure policy for Redis.** Dedupe and rate limiting both depend on it, so an outage halts sending (fail-closed). That is safe against duplicates but may be wrong for OTP traffic.
6. **Idempotency keys expire after 24 hours.** A campaign resumed after that can send again; extend the TTL for campaign keys.
7. **Consumer scaling stops at the partition count** (48 on the hot topics). Plan repartitioning before traffic nears that.
8. **Add jitter to retry delays** so that messages that failed together do not retry together.

# Notification Platform

A high-load notification service: push, email, SMS and in-app messages, immediate or scheduled, with preferences, templates, retries, a dead-letter queue, bulk campaigns and delivery tracking. Written in Go on Kafka, DynamoDB and Redis.

The original whiteboard design and the reasoning behind each change are in [docs/architecture.md](docs/architecture.md). Measured results are in [docs/benchmarks.md](docs/benchmarks.md), and Kubernetes notes in [docs/kubernetes.md](docs/kubernetes.md).

## How a notification flows

```
producer -> api -> DynamoDB (notification + idempotency key in one transaction) -> 202
                       | DynamoDB Streams
                     relay -> Kafka notifications.requested
                                  |
   scheduler <- send_at / quiet hours - router (preferences, caps, expiry, templates)
                                  |
                  dispatch.<channel>.<high|low>  (one topic per channel and priority)
                                  |
                 worker --channel X (rate limit, circuit breaker, dedupe)
                   | transient failure -> retry tiers -> DLQ (dlqctl replay)
                   v
        provider (FCM/APNs, SMTP, SMS, inbox + WebSocket)
                   v
        notifications.status <- provider webhooks -> status service -> DynamoDB
```

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

Real providers are not implemented: push, SMS and email go to the mock provider and Mailpit. PII and device tokens are stored unencrypted. Kubernetes manifests were never applied to a cluster. The 23K/s peak, a 30 minute soak and a Redis restart were not tested (see the benchmark report for what was).

Recommended next steps, most valuable first:

1. **Cut status event amplification.** Each notification emits about 8 status events; at 23K/s that is roughly 180K events/s into DynamoDB, and it was the first thing to fall behind in testing. Drop the per-channel `QUEUED` event, coalesce in the worker, batch writes, or move the event stream to a store built for appends.
2. **Remove the batch barrier in consumers.** A batch waits for its slowest message before the next poll. Pipelining per partition would raise worker throughput without more pods.
3. **Reconcile notifications stuck at `SENT`.** If a provider callback never arrives, nothing ever marks the notification final. Add a sweeper that moves old `SENT` records to an explicit unknown state and alerts on the count.
4. **Put user-scoped auth in front of inbox and WebSocket.** They currently take a producer API key, which is fine server-to-server but must not reach mobile clients; issue short-lived per-user tokens.
5. **Decide the failure policy for Redis.** Dedupe and rate limiting both depend on it, so an outage halts sending (fail-closed). That is safe against duplicates but may be wrong for OTP traffic.
6. **Idempotency keys expire after 24 hours.** A campaign resumed after that can send again; extend the TTL for campaign keys.
7. **Consumer scaling stops at the partition count** (48 on the hot topics). Plan repartitioning before traffic nears that.

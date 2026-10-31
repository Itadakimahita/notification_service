# Observability

Three signals: metrics (Prometheus), traces (OpenTelemetry to Jaeger) and alerts (Prometheus rules). Every service exposes `/metrics`, `/healthz` and `/readyz` on its admin port.

## Reading latency: p50, p95, p99

Latency is recorded in histograms, so the dashboard can show any percentile.

| Percentile | Meaning | Use it to see |
|---|---|---|
| p50 | Half of the requests were faster | Typical experience |
| p95 | 95 of 100 were faster | Whether a visible minority is slow. The enqueue target (100 ms) is a p95 |
| p99 | 99 of 100 were faster | Tail problems such as GC pauses, lock contention and retries |

Averages hide all of these, which is why no panel shows one. When p50 is low but p99 is high, a few requests are being held up by something specific (a slow dependency, a retry, a stalled consumer batch) and it is worth looking at traces for those requests.

## Dashboard

Grafana, "Notification Platform" (`deploy/grafana/dashboards/notification-platform.json`, 44 panels). Do not edit the JSON by hand: change `scripts/gen_dashboard.py` and run it. `scripts/check_dashboard.py` evaluates every panel expression against a running Prometheus and fails if one is invalid.

| Row | Answers |
|---|---|
| Overview | Are we healthy right now? Accept rate, enqueue p95 against the 100 ms target, 5xx ratio, total lag, DLQ size, targets up |
| API | Latency p50/p95/p99 for the enqueue route and per route, errors by status, inbox API latency |
| Outbox relay and scheduler | Is the relay keeping up with accepted traffic? Do both own their shards? |
| Router | Routing time p50/p95/p99 and results (ok, retry, poison) |
| Workers and providers | Outcomes per channel, success ratio, provider latency p50/p95/p99 per channel, retries, dead letters, breaker and rate limiting |
| Status tracking and webhooks | Events versus writes (coalescing), webhook latency, status lag |
| Kafka | Lag per group and topic, lag growth, retry backlog, DLQ depth |
| In-app gateway | Live connections, pushed and dropped messages |
| Campaigns and mock provider | Campaign emission rate, mock provider traffic |
| Runtime | Targets up, goroutines, memory, CPU, GC pause, file descriptors |

## Metric catalogue

| Metric | Type | Labels | Emitted by | Meaning |
|---|---|---|---|---|
| `http_request_duration_seconds` | histogram | method, route, status | api, status, inapp | Request latency by route pattern (never raw paths) |
| `relay_published_total` | counter | | relay | Events published from DynamoDB Streams to Kafka |
| `relay_owned_shards` | gauge | | relay | Stream shards this instance leases |
| `router_processed_total` | counter | result | router | ok, retry, poison |
| `router_route_seconds` | histogram | | router | Time to route one request |
| `worker_send_total` | counter | channel, result | workers | sent, retry, dead, permanent, invalid_token, expired, duplicate, in_flight, rate_limited, breaker_open |
| `worker_send_seconds` | histogram | channel | workers | Provider call latency |
| `scheduler_emitted_total` | counter | | scheduler | Due requests re-emitted |
| `scheduler_owned_shards` | gauge | | scheduler | Schedule shards leased |
| `status_events_total` | counter | | status | Status events consumed |
| `status_updates_total` | counter | | status | DynamoDB updates after coalescing |
| `campaign_emitted_total` | counter | | campaign | Recipients accepted |
| `inapp_connections` | gauge | | inapp | Open WebSocket connections |
| `inapp_pushed_total` | counter | | inapp | Messages written to live connections |
| `inapp_dropped_total` | counter | | inapp | Messages dropped because a client was too slow |
| `mockprovider_requests_total` | counter | kind, code | mockprovider | Local runs only |
| `kafka_consumer_lag` | gauge | group, topic | lagexporter | Messages behind the log end |
| `notification_dlq_depth` | gauge | topic | lagexporter | Messages retained in a DLQ topic (replay does not delete) |

Go runtime and process metrics (`go_*`, `process_*`) come from the client library in every service.

## Traces

The API starts a span `notification.accept` and stores its W3C `traceparent` on the notification record. The relay copies it into a Kafka header, and the router and workers continue the trace (`router.route`, `worker.send`), re-injecting the context into everything they produce. One notification therefore appears as one trace across the whole pipeline, including the hop through DynamoDB Streams. Set `OTEL_EXPORTER_OTLP_ENDPOINT` (for example `localhost:4317`) to export; without it tracing is a no-op.

## Alerts

`deploy/prometheus/alerts.yml`:

| Alert | Fires when |
|---|---|
| ApiEnqueueLatencyHigh | enqueue p95 above 100 ms for 5 minutes |
| ApiErrorRateHigh | more than 1% of API requests are 5xx |
| ConsumerLagHigh | a consumer group is more than 100k messages behind |
| DeadLetterGrowing | a DLQ topic grew within 15 minutes |
| ProviderFailureRatioHigh | more than 20% of a channel's sends fail |
| ProviderBreakerOpen | a provider circuit breaker is open |
| RelayHasNoShards | no relay instance owns a stream shard. DynamoDB Streams keep records for 24 hours, so this must be fixed within a day or accepted notifications are lost |
| SchedulerHasNoShards | delayed notifications will not fire |
| ServiceDown | a scrape target is unreachable |

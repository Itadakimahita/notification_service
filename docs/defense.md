# Defense guide

How to run the project, what to run during the defense, and what to say. Every command below was run on the project machine while writing this guide, and the outputs shown are real.

Contents: [1. Before the day](#1-before-the-day) · [2. Start everything](#2-start-everything) · [3. Browser tabs](#3-browser-tabs-to-open) · [4. The demo, step by step](#4-the-demo-step-by-step) · [5. What to say](#5-what-to-say) · [6. Numbers you can quote](#6-numbers-you-can-quote) · [7. Questions you will get](#7-questions-you-will-get) · [8. What not to claim](#8-what-not-to-claim) · [9. If something breaks](#9-if-something-breaks)

---

## 1. Before the day

Do this at least a day ahead. Image pulls and the first Go build can take many minutes on a busy laptop.

1. Start Docker Desktop and wait until it is idle.
2. Check the toolchain: `go version` (the module needs Go 1.26; an older Go downloads it on first build, which needs internet).
3. Download everything:
   ```sh
   cd /Users/dika/Documents/highload/notification_service
   docker compose -f deploy/compose/docker-compose.yml pull   # includes the S3 image, which was the slowest
   go build ./...
   ```
4. Do one full rehearsal of section 4. Reset afterwards (see section 9).
5. Close heavy applications (Teams, Spotify, browsers with many tabs). During testing, the laptop's load average reached 8 to 17 and the Docker VM used up to 185% of a core. Latency in the live demo depends directly on this.
6. Port conflicts: the project uses 8081, 8082, 8083, 6380, 9300, 9090, 3000, 16686, 8025, 8090, 8001 and the 91xx admin ports. If another container holds one, stop that container or change the port; the project's defaults were already shifted away from clashes with other software on this machine.

## 2. Start everything

Use two terminals. In terminal A (keep it open):

```sh
cd /Users/dika/Documents/highload/notification_service
export RETRY_TIERS=1s,2s,3s,4s                       # short retry delays so the outage demo takes seconds, not hours
export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317    # sends traces to Jaeger

make up          # Kafka (3 brokers), DynamoDB Local, Redis, Mailpit, S3Mock, Prometheus, Grafana, Jaeger
make migrate     # expected: "dynamodb tables ready", "s3 bucket ready", "kafka topics ready"
make run         # builds and starts 13 processes in the background, logs in .run/
```

Then wait about 30 seconds. The consumers must join their Kafka groups before the first message is delivered; sending earlier works but delivery looks stalled. Check that everything is up:

```sh
for p in 9101 9102 9103 9104 9105 9106 9107 9108 9109 9110 9114 9124 9134; do curl -s -o /dev/null -w "$p=%{http_code} " localhost:$p/healthz; done; echo
# expect: 200 for all 13
```

Create the demo data once:

```sh
./scripts/demo.sh seed
```

Terminal B is for the live in-app feed during the demo:

```sh
cd /Users/dika/Documents/highload/notification_service
./scripts/demo.sh ws        # prints every in-app message for the demo user as it arrives
```

Stop everything afterwards with `make stop` (the services) and `make down` (the infrastructure, wipes data).

## 3. Browser tabs to open

| Tab | URL | Show |
|---|---|---|
| Grafana | http://localhost:3000 (no login) → dashboard "Notification Platform" | Latency p50/p95/p99, outcomes, lag, DLQ |
| Jaeger | http://localhost:16686 → service `api` → Find Traces | One trace across services |
| Mailpit | http://localhost:8025 | The email that was sent |
| Kafka UI | http://localhost:8090 | Topics: dispatch lanes, retry tiers, DLQ |
| Prometheus | http://localhost:9090/alerts | The 9 alert rules |
| The README | on GitHub or in the editor | The diagrams |

## 4. The demo, step by step

All steps use `./scripts/demo.sh <step>`. A full run is about 15 minutes; the sections marked *short version* fit in 8.

### Step 1: one notification, three channels (*short version*)
```sh
./scripts/demo.sh send
```
Real output:
```
accepted in 49 ms: {"notification_id":"01a1902a-473b-...","duplicate":false}
overall: ROUTED
email  SENT
inapp  DELIVERED
push   DELIVERED
mailpit: ... latest to demo@example.com, subject 'Hello Defense'
```
Show: the 202 came back in tens of milliseconds, before anything was sent. Terminal B printed the in-app message live. Mailpit shows the email. Push became `DELIVERED` because the provider called our webhook. Email stops at `SENT` because SMTP gives no delivery receipt. Point at README diagram 2.

### Step 2: idempotency (*short version*)
```sh
./scripts/demo.sh idem
```
The second call returns the same `notification_id` with `"duplicate":true`. A client retrying after a timeout cannot send twice.

### Step 3: scheduled delivery
```sh
./scripts/demo.sh schedule 15
```
Four seconds in, the status is `SCHEDULED`; after the due time it becomes `DELIVERED`. Explain the sharded time buckets (README diagram 7).

### Step 4: a bad device token
```sh
./scripts/demo.sh badtoken
```
Shows `push FAILED (invalid token)`. The provider rejected the token and the platform deactivated the device, so it is never tried again.

### Step 5: provider outage, retries, dead letters and replay (*short version*)
```sh
./scripts/demo.sh outage
```
Takes about 40 seconds. Real output:
```
provider now fails 100% of calls
worker_send_total{channel="push",result="retry"} 4
worker_send_total{channel="push",result="dead"} 1
push   FAILED   (retries exhausted: provider status 503)
dead-letter queue: ... notification=... attempt=5 error="provider status 503"
provider recovered
1 message(s) replayed to dispatch.push
push   DELIVERED
```
Show: four retries through the delay tiers, then the dead-letter queue, then recovery and a replay that delivers it. While it runs, switch to Grafana panels "Outcomes by channel and result" and "Dead-letter depth by topic". Point at README diagram 5.

### Step 6: follow one notification as a trace (*short version*)
Use the id printed by step 1 (`./scripts/demo.sh send` prints it). In Jaeger choose service `api`, click Find Traces, open the latest trace. It shows `notification.accept` in the API, `router.route`, and `worker.send` for each channel, in one trace. The trace crosses the DynamoDB Streams hop and Kafka through message headers.

### Step 7: bulk campaign
```sh
./scripts/demo.sh campaign
```
Real output: the campaign is created, 100 rows are uploaded straight to object storage through a presigned URL, and progress goes `RUNNING emitted 25` → `75` → `COMPLETED emitted 100`. It is throttled at 50 per second and resumable after a crash.

### Step 8: quiet hours (optional, about 1 minute)
```sh
./scripts/demo.sh quiet
```
A marketing message arrives during the user's quiet window, shows `inapp SCHEDULED (quiet hours)`, and is delivered when the window ends. Transactional messages ignore quiet hours.

### Step 9: the dashboard (*short version*)
Open Grafana and walk through: the Overview row (accept rate, enqueue p95 against the 100 ms line, lag, DLQ), "Enqueue latency p50 / p95 / p99", "Provider call latency p95 (by channel)", "Consumer lag by group and topic". Explain that p50 is the typical request, p95 is the number the requirement is written against, and p99 shows the tail. Averages are deliberately not shown.

### Step 10: load, honestly
```sh
./scripts/demo.sh load        # 50 requests per second for 20 s, then checks delivery of a sample
```
At 50 per second with every service running, the accept p95 was 86 ms (target 100 ms). Do **not** raise the rate live: at 100 per second it was 170 ms (partly while an earlier run was still draining), and at 300 per second it was 1.1 s, because a laptop running Kafka, DynamoDB Local, Redis and 13 processes is out of CPU. The delivery check still passed at 300 per second (300 of 300 delivered). Say this openly and show the isolated numbers instead (section 6): with the downstream stopped, the accept path did 2,000 per second with p95 114 ms.

### Step 11: failures you can cause live (optional)
`make chaos` runs three scenarios and prints a delivery check after each (about 5 minutes; use the recorded results in `docs/benchmarks.md` if short on time). To show one by hand while load runs in another terminal:
```sh
go build -o bin/loadgen ./cmd/loadgen && bin/loadgen -rate 50 -duration 40s -channels push -verify 300 &   # terminal B
pkill -9 -f 'bin/worker --channel push'                              # kill the push worker
(ADMIN_ADDR=:9104 bin/worker --channel push > .run/worker-push.log 2>&1 &)   # restart it
```
Everything accepted is still delivered after the restart. A few messages may be sent twice (the system is at-least-once), which you should say before anyone asks.

## 5. What to say

A suggested order (20 minutes total, trim the demo for 10):

1. **Problem and targets (1 min).** 100M daily users, 200M notifications a day (2.3K/s average, 23K/s peak), enqueue p95 under 100 ms, never lose an accepted notification.
2. **What I changed from the original design (2 min).** Show the original whiteboard image in the README, then:

   | Gap in the original | What I did |
   |---|---|
   | Workers did everything | Added a router; workers only send |
   | One queue for all channels | One topic per channel and priority, so a slow SMS provider cannot block push and marketing cannot starve codes |
   | Retries blocked the consumer | Retry tiers as separate topics, then a DLQ with replay |
   | Scheduler floated on its own | Scheduler owns `send_at` and quiet hours, in sharded time buckets |
   | Outbox was unclear on DynamoDB | Outbox through DynamoDB Streams, so there is no dual write |
   | Preferences per channel only | Per category and channel, transactional versus marketing, quiet hours, daily caps |
   | Stale messages could be sent | `expires_at` on every notification |
   | In-app had no transport | Inbox table plus WebSocket gateway |
   | Every attempt hit the database | Only the latest status per channel, with a rank so it never goes backwards |
   | Bulk sends used the normal API | A campaign service with presigned upload, throttling and resume |
   | Dead tokens stayed | The provider's "invalid token" deactivates the device |

   These ideas follow what large public systems describe (LinkedIn's Air Traffic Controller between producers and channels, Netflix RENO's priority queues, Uber's Kafka retry and dead-letter design). I know them from public engineering write-ups, so check the original posts before quoting details.
3. **Architecture (3 min).** README diagram 1, then diagram 2. Emphasise that acceptance only needs DynamoDB.
4. **Live demo (8 min).** Section 4.
5. **Results (2 min).** Section 6.
6. **Decisions (2 min).** Open `docs/decisions/README.md`: every choice has a file with why it is better and what it costs.
7. **Limitations and next steps (2 min).** Section 8 and the list at the end of the README. Naming your own weaknesses before the committee does is the strongest position.

## 6. Numbers you can quote

All from `docs/benchmarks.md`, measured on one laptop with DynamoDB Local. They describe this build locally, not production capacity.

| Claim | Number |
|---|---|
| Accept path, downstream stopped | 500/s: p50 1.3 ms, p95 16 ms, p99 129 ms. 2,000/s: p95 114 ms. 4,000/s saturates (3,406/s achieved, 1,180 failures) |
| One API instance ceiling here | about 2,000 requests per second |
| Full stack under load | p95 1.5 s at 500/s on this laptop. Target not met in that configuration, because everything competes for one CPU and one DynamoDB Local |
| Delivery under 500/s | 989 of 1,000 sampled delivered; the other 11 were provider callbacks the mock lost, since fixed |
| Chaos: provider 50% errors and 2 s latency | 500 of 500 delivered |
| Chaos: worker `kill -9` | 500 of 500 delivered; 2,004 provider sends for 2,000 notifications (4 duplicates) |
| Chaos: Kafka broker stopped 15 s | API accepted 2,000 of 2,000; 500 of 500 delivered |
| Downstream drain | relay and router moved about 124,000 events in about 20 s; push worker 850 to 1,100/s; in-app about 170/s (DynamoDB Local) |

## 7. Questions you will get

**Why Kafka and not RabbitMQ or SQS?** A replayable partitioned log gives per-user ordering, visible backpressure (lag), and DLQ replay. Details and the cost (partition planning, no delayed delivery) are in `docs/decisions/002-kafka.md`. The comparison with the other brokers is reasoning, not a benchmark.

**How do you guarantee a notification is not lost?** Acceptance means it is durably stored in DynamoDB, together with its idempotency key. A relay publishes it and retries until Kafka acknowledges. Offsets are committed only after outputs exist. I demonstrated this by stopping a Kafka broker during load: everything accepted was delivered.

**Is it exactly-once?** No, and nothing across a database, Kafka and an external provider can be. It is at-least-once with duplicates made harmless at each boundary. The remaining window is a crash after the provider accepted a message but before the sent marker was written; the worker-kill test measured 4 duplicates in 2,000.

**Can it handle 23K requests per second?** I did not test that. One API instance did about 2,000 per second here, so the peak needs roughly a dozen instances and real DynamoDB, and the stages behind it scale by partition count (48 on the hot topics). The scaling design is in `docs/kubernetes.md` but was never run on a cluster.

**What is the bottleneck?** Status events. Each notification emits about 8, which at 23K/s would be roughly 180K per second into DynamoDB; the status topic fell about 700,000 messages behind in my test. The fix list is in `docs/decisions/010-status-as-events.md`.

**What if Redis goes down?** Sending stops, because dedupe and rate limiting both need it (fail-closed). That prevents duplicates but delays everything, which may be wrong for one-time codes. Not tested.

**What if DynamoDB Streams is down for a long time?** Streams keep records for 24 hours. A relay outage longer than that loses accepted notifications, which is why the `RelayHasNoShards` alert exists.

**How do you avoid one slow provider blocking everything?** Separate topics and consumer groups per channel and priority, a circuit breaker per provider, per-lane rate limits, and retry tiers that never stall a partition. The isolation between a bulk campaign and transactional traffic is designed but I did not measure it under mixed load.

**How does it stay correct when events arrive out of order?** Status updates carry a rank and only succeed if they raise it. A late `QUEUED` cannot overwrite `DELIVERED`. A notification replayed from the DLQ can still move from `FAILED` to `SENT`, because failed ranks below sent.

**Why mock providers?** Real FCM, APNs, SMS and email providers need accounts and cost money. Each is behind one interface (`internal/channels`), so a real adapter is a contained change. The mock also lets me inject failures and latency for the chaos tests.

**Security and privacy?** API keys per producer, constant-time webhook key checks, containers running as non-root. Not done: encryption of device tokens and personal data, and user-scoped authentication for the inbox and WebSocket endpoints, which currently take a producer key.

**How did you test it?** Unit tests with fakes, integration tests against real DynamoDB Local, Kafka and Redis (they skip when the stack is not running), a load generator that verifies delivery, and chaos scenarios. Measuring also found real bugs: duplicate publishes on shutdown in the relay and scheduler, a router that hung on shutdown, and a Redis client pointed at the wrong address. They are listed in the benchmark notes and the commit history.

**What would you do differently?** Reduce the status event volume first, pipeline the consumer loops so one slow message does not hold a batch, add jitter to retry delays, and reconcile notifications that stay at `SENT` when a provider callback never arrives.

## 8. What not to claim

- Do not say it handles 23K requests per second, or that it is production-ready.
- Do not say exactly-once.
- Do not say the Kubernetes setup works on a cluster. The manifests render and pass consistency tests; they were never applied.
- Do not say the isolation between campaigns and transactional traffic is proven under load. It is designed in, not measured.
- Do not present the full-stack latency as meeting the target. The isolated accept path does; the shared-laptop run does not.
- Real providers, PII encryption and user-scoped auth are not built. Say so before you are asked.

## 9. If something breaks

| Symptom | Fix |
|---|---|
| A step shows no delivery for 20 to 40 s after `make run` | Consumers are still joining their Kafka groups. Wait and retry |
| `make migrate` hangs for about 2 minutes then fails | The S3 container is not up. `docker compose -f deploy/compose/docker-compose.yml up -d s3`, or run `S3_ENDPOINT= make migrate` to skip the bucket |
| Everything is slow, tests time out | DynamoDB Local degrades after large runs. `make stop`, `docker restart notif-dynamodb-1`, `make migrate`, `make run`, `./scripts/demo.sh seed` |
| A port is already in use | Another container holds it. Find it with `lsof -nP -iTCP:<port> -sTCP:LISTEN`, and stop it or change the port |
| `outage` takes minutes | `RETRY_TIERS` was not exported before `make migrate` and `make run`. Stop, export it, start again |
| After a crash, services will not take messages for about 45 s | Kafka is waiting for the dead consumer's session to expire. Wait |
| Grafana shows "No data" on a panel | Some panels need traffic (errors, circuit breaker, inbox calls). Run `./scripts/demo.sh send` and `outage`, then refresh |
| A demo step prints a Python error | The service it calls is not up. Check the 13 health URLs from section 2 |

Full reset between rehearsals:
```sh
make stop
docker restart notif-dynamodb-1
export RETRY_TIERS=1s,2s,3s,4s OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317
make migrate && make run
sleep 30 && ./scripts/demo.sh seed
```

## Evidence you can run if asked

```sh
make test                              # unit and integration tests (stack must be up)
go test ./deploy/...                   # Kubernetes manifest consistency
python3 scripts/check_dashboard.py     # every Grafana expression is valid against live Prometheus
make chaos                             # three failure scenarios with delivery verification
```
Repository map: `cmd/` one folder per service, `internal/` shared packages, `deploy/` compose, Prometheus, Grafana and Kubernetes, `docs/decisions/` the reasoning, `docs/benchmarks.md` the measurements.

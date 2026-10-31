# Benchmarks and chaos results

These numbers come from one developer laptop and say what this build does locally. They are not a capacity claim for production.

**Environment:** macOS arm64, 8 cores shared with desktop applications (system load average 8 to 17 during the runs). One instance of each service on the host. DynamoDB Local (in-memory), a 3-broker Kafka cluster and Redis run in the Docker VM, which used up to about 185% of a core. Tools: `cmd/loadgen` (open-loop arrival rate, latency percentiles, delivery verification) and `loadtest/chaos.sh`. A k6 script for larger setups is in `loadtest/k6/notify.js`.

## What was and was not run

| Planned | Status |
|---|---|
| Sustained load, accept path | Run at 500, 2000 and 4000 req/s |
| 23K/s burst | **Not run.** One laptop with DynamoDB Local cannot generate or absorb it |
| 30 minute soak | **Not run** |
| Campaign plus transactional mix | **Not run** (campaign runner is covered by tests and a protocol-level integration test) |
| Provider at 50% errors and high latency | Run, passed |
| Kill a worker | Run, passed |
| Stop a Kafka broker | Run, passed |
| Redis restart | **Not run** |

## Accept path (API plus DynamoDB Local, downstream stopped)

Measures `POST /v1/notifications`: validate, idempotency check and notification write in one DynamoDB transaction.

| Target | Achieved | p50 | p95 | p99 | Failures |
|---|---|---|---|---|---|
| 500/s | 500/s | 1.3 ms | 16 ms | 129 ms | 0 |
| 2000/s | 2000/s | 0.6 ms | 114 ms | 434 ms | 0 |
| 4000/s | 3406/s | 405 ms | 3.4 s | 8.7 s | 1180 (95 HTTP 500, 1085 client timeouts) |

The p95 target of 100 ms holds comfortably at 500/s. At 2000/s one instance is right at the limit. At 4000/s it saturates, so the average load in the requirements (2.3K/s) is about one instance here, and the 23K/s peak needs horizontal API scaling plus real DynamoDB.

## Full stack at 500 req/s

Everything running and competing for the same CPU and the same DynamoDB Local: accept p50 430 ms, p95 1.5 s, 15,000 of 15,000 accepted. The server-side histogram showed the same latency, so it is contention and not client overhead. **The 100 ms target was not met in this configuration.**

Delivery check on 1,000 sampled notifications: 989 reached `DELIVERED` on both channels. The 11 others stayed at `SENT` because the mock provider fired its delivery callback once and ignored failures, losing about 0.7% of callbacks (the status service received 37,499 callbacks for 37,749 sends). The mock now retries with backoff like a real provider. Stage counters matched exactly (relay, router, push and in-app all at 37,749), so no message was lost in transport.

## Downstream drain (about 124,000 queued notifications)

- Relay plus router moved the whole backlog in roughly 20 seconds (several thousand events per second).
- Push worker: about 850 to 1,100 sends per second.
- In-app worker: about 170 per second, limited by inbox writes to DynamoDB Local.
- Status updates fell far behind: the status topic reached a lag of about 706,000 events. Each notification emits roughly 8 status events and a single DynamoDB Local applies only a few hundred updates per second.

## Chaos scenarios (push channel, 100 req/s for 20 s, 500 sampled for verification)

| Scenario | Accepted | Delivered | Unaccounted |
|---|---|---|---|
| Provider at 50% errors and 2 s latency | 2000 | 500 of 500 | 0 |
| `kill -9` of the push worker, restarted 5 s later | 2000 | 500 of 500 | 0 (provider saw 2004 sends for 2000 notifications) |
| Kafka broker stopped for 15 s | 2000 | 500 of 500 | 0 |

The 4 extra provider sends in the worker-crash run are the at-least-once window: a message sent just before the crash is redelivered after restart. The dedupe marker narrows this window but cannot close it.
During the broker outage the API was unaffected, because acceptance only needs DynamoDB. The outbox relay publishes once Kafka returns.

## Problems found while measuring

1. **Port clash.** Another container held `127.0.0.1:8080`, so about a third of requests to `localhost` got 404. The API now listens on 8081.
2. **Lost provider callbacks** in the mock (fixed, see above).
3. **Status event amplification.** About 8 events per notification at 23K/s would be roughly 180K status events per second. See the recommendations in the project notes.
4. **Batch barrier in consumers.** `ProcessBatches` waits for the slowest record of a batch before polling again, so one slow call stalls 199 others. Per-partition pipelining would raise worker throughput.
5. **DynamoDB Local degrades after large runs.** After roughly 140,000 notifications and about a million status events, the integration tests slowed from seconds to a minute and one timed out. Restarting the container (it is in-memory) restored normal timings, so reset it between load runs: `docker restart notif-dynamodb-1 && make migrate`.

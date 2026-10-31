# 005. A router stage and one topic per channel and priority

**Status:** accepted

## Context
The original design had workers consume one queue and do everything: look up preferences, render templates and call providers. One slow provider, or one bulk marketing run, would delay everything behind it.

## Decision
Insert a router between acceptance and delivery. The router applies policy once (expiry, opt-out, quiet hours, frequency caps, templates, device and address resolution) and writes ready-to-send messages to separate topics per channel and priority: `notifications.dispatch.<channel>.<high|low>`. Workers only send.

## Why this is better
- **Bulkheads.** A slow SMS provider backs up only `dispatch.sms.*`. Push and email keep flowing.
- **Priority by construction.** OTP-style transactional messages use the high lane with its own consumer group and its own share of the provider rate limit (70/30 by default). Bulk marketing goes to the low lane and cannot sit in front of them.
- **Workers stay simple and fast.** They hold no preference or template logic, so they scale on sending capacity alone and are easy to reason about during incidents.
- **Policy lives in one place.** A rule change touches the router, not four workers.

## What we give up
- One more hop and one more service to run and watch.
- Many topics (31 in total, including retry tiers). Partition counts must be planned for each.

## Evidence
Measured: the lane topics, per-lane consumer groups and rate-limit shares work end to end in the e2e runs. **Not measured:** the claim that a bulk campaign leaves transactional latency unaffected. That scenario ("campaign plus OTP mix") was planned but not run (`docs/benchmarks.md`). Treat the isolation benefit as designed, not yet demonstrated under load.

## Revisit when
Measurements show the extra hop costs more than the isolation is worth for a given channel.

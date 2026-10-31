# 007. At-least-once delivery with layered idempotency

**Status:** accepted

## Context
Exactly-once delivery across a database, Kafka and an external provider is not achievable. The practical choices are at-most-once (lose messages) or at-least-once (risk duplicates).

## Decision
Process at-least-once everywhere and make duplicates harmless at each boundary:
1. **API:** the `Idempotency-Key` is stored with the notification in one DynamoDB transaction. A repeat returns the original id and creates nothing.
2. **Campaigns:** the idempotency key is the user id within the campaign, so re-running or resuming cannot create a second notification for the same user.
3. **Worker:** before calling the provider it claims `sent:<notification>:<channel>:<target>` in Redis (`SET NX`). A "sent" marker means skip; a pending claim means another worker is mid-send, so the message is retried shortly. Failures release the claim.
4. **Provider keys where supported:** a stable `Message-ID` for email and a collapse id for push.

## Why this is better
- **No lost messages.** Offsets are committed only after outputs are produced, status is recorded and failures go to retry or the DLQ.
- **Duplicates are rare and bounded.** Each layer closes most of the window the previous one leaves open.
- **Cheap.** One Redis operation per send and one extra item per accepted request.

## What we give up
- **A small duplicate window remains**: a crash after the provider accepted a message but before the "sent" marker is written. Measured: a `kill -9` of the push worker during load produced 2,004 provider sends for 2,000 notifications.
- The API idempotency record expires after 24 hours. A client retrying later, or a campaign resumed after a day, can send again.
- If Redis is down the worker cannot claim, so sending halts rather than risk duplicates (fail-closed). That may be the wrong trade for OTP traffic.

## Evidence
Measured: idempotent replay (API and campaign) in integration tests, dedupe states against real Redis, the crash duplicates above.

## Revisit when
The 24 hour window is too short for some producers (extend the TTL), or sending during a Redis outage matters more than avoiding duplicates.

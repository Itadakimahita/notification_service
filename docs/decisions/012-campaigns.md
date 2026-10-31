# 012. Bulk campaigns go through the normal accept path

**Status:** accepted

## Context
A campaign can reach millions of users. It must not overload the system, must be pausable, must survive a crash without re-sending to everyone, and its recipients still need tracked status.

## Decision
The producer uploads the audience CSV straight to object storage using a presigned URL. A campaign runner streams the file by byte offset in rate-limited chunks and, for each row, creates a real notification exactly as the API would (priority low, idempotency key equal to the user id within the campaign). After each chunk it saves its offset and counters and re-reads the campaign status, so pause and cancel take effect at the next chunk boundary.

## Why this is better
- **Everything downstream is reused.** The relay, router, lanes, status tracking and idempotency all apply unchanged, so campaign recipients have the same observable status as any notification.
- **Resume cannot double-send.** A crash replays at most one chunk, and the per-user idempotency key turns the replay into no-ops (tested, including re-running the same file).
- **The API never carries the bytes.** Presigned upload keeps large files off the request path.
- **Throttled and isolated.** Rate per second is part of the campaign, and the low lane keeps bulk traffic away from transactional messages.

## What we give up
- **Roughly double the writes** compared with publishing to Kafka directly (notification plus idempotency record per recipient).
- Idempotency keys expire after 24 hours, so resuming a campaign after that can resend to users already reached.
- Running campaigns are found with a table scan, fine for dozens and not for thousands.
- No automatic backpressure from consumer lag yet. The rate is fixed per campaign.

## Evidence
Measured: unit tests with fakes (parsing, column overrides, pause and resume without gaps or duplicates, cancel, missing file, crash resume, throttling) and an integration test against real DynamoDB and an in-process S3 server. A live run also completed: a 100 recipient campaign uploaded through a presigned URL to the S3Mock container ran to `COMPLETED` with 100 emitted. **Not run:** a campaign at volume, and the campaign plus transactional mix.

## Revisit when
Audiences reach tens of millions (consider a direct-to-Kafka path with a separate dedupe), or lag-based backpressure is needed.

# 013. Protobuf for Kafka messages

**Status:** accepted

## Context
Services communicate only through Kafka messages, so the message format is the contract between teams and releases.

## Decision
Define events in `proto/notification/v1/events.proto`, generate Go types with `buf`, and lint with `buf lint`. Messages are `NotificationRequested`, `DispatchMessage` and `StatusEvent`.

## Why this is better
- **Compact and typed.** Smaller than JSON at tens of thousands of messages per second, and no hand-written parsing.
- **Safe evolution.** Adding fields is backward compatible, and field numbers make renames harmless.
- **Cheap to copy.** `proto.Clone` is used for retry copies. (It also caught a bug: copying the struct by value copied an internal mutex, which `go vet` flagged.)

## What we give up
- Messages are unreadable in Kafka UIs without decoding.
- No schema registry enforces compatibility at publish time. `buf breaking` in CI would cover this and is not set up yet.
- Provider webhooks and the public API stay JSON, because those contracts belong to other parties.

## Evidence
Reasoned plus working code: all stages exchange these messages in every end-to-end run. No size or CPU comparison with JSON was measured.

## Revisit when
Multiple independent teams publish to the same topics (add a registry and `buf breaking` checks).

# 016. Policy model: categories, preferences, quiet hours and caps

**Status:** accepted

## Context
Users must be able to control what they receive, but a password reset or a one-time code must not be blocked by a marketing preference or a night-time quiet window.

## Decision
Each notification has a category: `transactional` or `marketing`. A user's preferences are stored per category and channel, with an optional quiet window in their time zone. The router applies, in order: expiry, opt-out, quiet hours (marketing only, deferred per channel to the end of the window), recipient availability (devices, email, phone), and a daily frequency cap for marketing (default 3). If no channels are requested, push, in-app and email are used. SMS must be asked for explicitly because it costs money.

## Why this is better
- **Safety for critical messages.** Transactional traffic skips quiet hours and marketing caps, and has its own high-priority lane.
- **Fatigue control is automatic.** The cap counts per user per day without any producer cooperation, and does not double-count redeliveries.
- **Per-channel decisions.** A user can mute email marketing but keep push, and one channel in quiet hours does not delay the others.
- **Freshness.** `expires_at` stops a stale code from being delivered after it stopped being useful.
- **Auditable.** Each suppression records a reason (opted out, quiet hours, frequency cap, no email address) on the notification's channel status.

## What we give up
- An explicit opt-out of a transactional channel is honoured, which a business might not want for legally required messages. That is a policy decision to confirm.
- Email addresses and phone numbers are not verified here.
- Caps are per user per UTC day, not per local day.

## Evidence
Measured: unit tests for every branch of the router and the quiet-hour window math (overnight, same day, time zones), plus live runs of quiet-hour deferral and `send_at`.

## Revisit when
Legal requirements define categories that can never be opted out, or caps need to follow the user's local day.

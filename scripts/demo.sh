#!/usr/bin/env bash
# Scripted demo steps for the notification platform. Run `scripts/demo.sh help` for the list.
set -uo pipefail
cd "$(dirname "$0")/.."

API=${API_URL:-http://localhost:8081}
KEY=${API_KEY:-demo-key}
MOCK=${MOCK_URL:-http://localhost:9200}
MAILPIT=${MAILPIT_URL:-http://localhost:8025}
USER_ID=${DEMO_USER:-demo-user}

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
call() { curl -s -H "Authorization: ApiKey $KEY" "$@"; }
post() { local path=$1 body=$2; shift 2; call -XPOST "$API$path" -d "$body" "$@"; }
put() { call -XPUT "$API$1" -d "$2" -o /dev/null -w "  %{http_code}\n"; }

# utc_shift +15S / -1H / +1M +FORMAT prints the shifted UTC time (works on macOS and GNU date)
utc_shift() {
  if date -v+1S >/dev/null 2>&1; then date -u -v"$1" "$2"; else
    n=${1:1}; sign=${1:0:1}; unit=${n: -1}; num=${n%?}
    case $unit in S) w=seconds;; M) w=minutes;; H) w=hours;; esac
    date -u -d "${sign}${num} ${w}" "$2"
  fi
}

id_of() { python3 -c 'import sys,json; print(json.load(sys.stdin)["notification_id"])'; }

show() {
  call "$API/v1/notifications/$1" | python3 -c '
import sys, json
d = json.load(sys.stdin)
print("  overall:", d.get("status"))
for ch, st in sorted((d.get("channel_status") or {}).items()):
    det = (d.get("channel_detail") or {}).get(ch, "")
    print(f"  {ch:6} {st}" + (f"   ({det})" if det else ""))'
}

# wait_final ID [SECONDS]: polls until every channel is terminal (SENT counts for email), then prints the status
wait_final() {
  local id=$1 secs=${2:-40}
  for _ in $(seq 1 "$secs"); do
    done_=$(call "$API/v1/notifications/$id" | python3 -c '
import sys, json
d = json.load(sys.stdin); cs = d.get("channel_status") or {}
term = {"DELIVERED", "FAILED", "SUPPRESSED", "EXPIRED"}
print("yes" if cs and all(s in term or (c == "email" and s == "SENT") for c, s in cs.items()) else "no")')
    [ "$done_" = yes ] && break
    sleep 1
  done
  show "$id"
}

mailpit_latest() {
  curl -s "$MAILPIT/api/v1/messages" | python3 -c '
import sys, json
d = json.load(sys.stdin)
m = d["messages"][0] if d["messages"] else None
print("  mailpit: %d message(s)" % d["total"] + (", latest to %s, subject %r" % (m["To"][0]["Address"], m["Subject"]) if m else ""))'
}

cmd_seed() {
  say "templates (push, email, in-app), a user with email and an iOS device, and a user whose only device has an invalid token"
  for ch in push email inapp; do
    post /v1/templates "{\"template_id\":\"welcome\",\"channel\":\"$ch\",\"locale\":\"en\",\"subject\":\"Hello {{.name}}\",\"body\":\"Welcome, {{.name}}!\"}" -o /dev/null -w "  template $ch: %{http_code}\n"
  done
  put "/v1/users/$USER_ID" '{"locale":"en","timezone":"UTC","email":"demo@example.com"}'
  post "/v1/users/$USER_ID/devices" '{"token":"demo-ios-token","platform":"ios"}' -o /dev/null -w "  device: %{http_code}\n"
  put "/v1/users/demo-badtoken" '{"locale":"en","timezone":"UTC"}'
  post "/v1/users/demo-badtoken/devices" '{"token":"invalid-expired-token","platform":"android"}' -o /dev/null -w "  bad device: %{http_code}\n"
}

cmd_send() {
  say "send one transactional notification to push, email and in-app"
  local body="{\"user_id\":\"$USER_ID\",\"template_id\":\"welcome\",\"category\":\"transactional\",\"data\":{\"name\":\"Defense\"}}"
  local start=$(python3 -c 'import time; print(time.time())')
  local out; out=$(post /v1/notifications "$body")
  local took=$(python3 -c "import time; print(round((time.time()-$start)*1000))")
  echo "  accepted in ${took} ms: $out"
  local id; id=$(echo "$out" | id_of)
  echo "  waiting for delivery..."; wait_final "$id"; mailpit_latest
  echo "  notification id (use it to find the trace in Jaeger): $id"
}

cmd_idem() {
  say "idempotency: the same Idempotency-Key twice creates one notification"
  local key; key=$(uuidgen); local body="{\"user_id\":\"$USER_ID\",\"template_id\":\"welcome\",\"category\":\"transactional\",\"channels\":[\"inapp\"],\"data\":{\"name\":\"Once\"}}"
  echo "  first : $(call -XPOST "$API/v1/notifications" -H "Idempotency-Key: $key" -d "$body")"
  echo "  second: $(call -XPOST "$API/v1/notifications" -H "Idempotency-Key: $key" -d "$body")"
  echo "  same notification_id, second one says duplicate:true"
}

cmd_schedule() {
  local secs=${1:-20}
  say "schedule: send_at in $secs seconds"
  local at; at=$(utc_shift "+${secs}S" +%Y-%m-%dT%H:%M:%SZ)
  local out; out=$(post /v1/notifications "{\"user_id\":\"$USER_ID\",\"template_id\":\"welcome\",\"category\":\"transactional\",\"channels\":[\"inapp\"],\"send_at\":\"$at\",\"data\":{\"name\":\"Scheduled for $at\"}}")
  local id; id=$(echo "$out" | id_of); echo "  accepted: $out"
  sleep 4; echo "  4 seconds later (not due yet):"; show "$id"
  echo "  waiting for the due time..."; wait_final "$id" $((secs + 40))
}

cmd_quiet() {
  say "quiet hours: a marketing message waits until the user's quiet window ends"
  local qs qe; qs=$(utc_shift -1H +%H:%M); qe=$(utc_shift +1M +%H:%M)
  put "/v1/users/$USER_ID/preferences" "{\"preferences\":[{\"category\":\"marketing\",\"channel\":\"inapp\",\"enabled\":true,\"quiet_start\":\"$qs\",\"quiet_end\":\"$qe\"}]}"
  echo "  quiet window (UTC) $qs to $qe"
  local out; out=$(post /v1/notifications "{\"user_id\":\"$USER_ID\",\"template_id\":\"welcome\",\"category\":\"marketing\",\"channels\":[\"inapp\"],\"data\":{\"name\":\"Marketing\"}}")
  local id; id=$(echo "$out" | id_of); sleep 4
  echo "  now (inside quiet hours):"; show "$id"
  echo "  waiting up to 90 s for the window to end..."; wait_final "$id" 90
  put "/v1/users/$USER_ID/preferences" '{"preferences":[{"category":"marketing","channel":"inapp","enabled":true}]}' >/dev/null
}

cmd_badtoken() {
  say "invalid push token: the provider rejects it and the device is deactivated"
  local out; out=$(post /v1/notifications '{"user_id":"demo-badtoken","template_id":"welcome","category":"transactional","channels":["push"],"data":{"name":"X"}}')
  local id; id=$(echo "$out" | id_of); wait_final "$id" 30
}

cmd_outage() {
  say "provider outage: retries, dead-letter queue, recovery and replay"
  if [ "${RETRY_TIERS:-}" != "1s,2s,3s,4s" ]; then
    echo "  WARNING: RETRY_TIERS is not 1s,2s,3s,4s, so retries take 5s, 30s, 5m, 30m and this demo will be slow." >&2
  fi
  go build -o bin/dlqctl ./cmd/dlqctl || return 1
  curl -s -XPOST "$MOCK/admin/config" -d '{"error_rate":1,"latency_ms":20}' >/dev/null && echo "  provider now fails 100% of calls"
  local out; out=$(post /v1/notifications "{\"user_id\":\"$USER_ID\",\"template_id\":\"welcome\",\"category\":\"transactional\",\"channels\":[\"push\"],\"data\":{\"name\":\"Outage\"}}")
  local id; id=$(echo "$out" | id_of); echo "  accepted: $out"
  echo "  waiting 25 s for the retry tiers to run out..."; sleep 25
  echo "  worker outcomes (retry = went to a retry tier, dead = went to the DLQ):"
  curl -s localhost:9104/metrics | grep '^worker_send_total' | sed 's/^/    /'
  show "$id"
  echo "  dead-letter queue:"; bin/dlqctl inspect --channel push | sed 's/^/    /'
  curl -s -XPOST "$MOCK/admin/config" -d '{"error_rate":0,"latency_ms":50}' >/dev/null && echo "  provider recovered"
  bin/dlqctl replay --channel push | sed 's/^/  /'
  wait_final "$id" 30
}

cmd_campaign() {
  say "bulk campaign: presigned upload, throttled run, progress"
  local csv=/tmp/demo-audience.csv
  python3 - "$USER_ID" > "$csv" <<'PY'
import sys
print("user_id,name")
print(f"{sys.argv[1]},Demo User")
for i in range(99):
    print(f"ghost-{i:03d},Ghost {i}")
PY
  local out; out=$(post /v1/campaigns '{"name":"defense-demo","template_id":"welcome","channels":["inapp"],"data":{"name":"there"},"rate_per_sec":50}')
  local cid url; cid=$(echo "$out" | python3 -c 'import sys,json; print(json.load(sys.stdin)["campaign_id"])'); url=$(echo "$out" | python3 -c 'import sys,json; print(json.load(sys.stdin)["upload_url"])')
  echo "  campaign $cid created, uploading 100 rows straight to object storage"
  curl -s -X PUT --data-binary @"$csv" "$url" -o /dev/null -w "  upload: %{http_code}\n"
  post "/v1/campaigns/$cid/start" '' -o /dev/null -w "  start: %{http_code}\n"
  for _ in $(seq 1 30); do
    line=$(call "$API/v1/campaigns/$cid" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["status"], "emitted", d["emitted"], "skipped", d["skipped"])')
    echo "  $line"; case "$line" in COMPLETED*|FAILED*) break;; esac; sleep 1
  done
}

cmd_load() {
  say "load test: ${RATE:-50} requests per second for 20 s, then verify delivery of a sample"
  go build -o bin/loadgen ./cmd/loadgen || return 1
  bin/loadgen -url "$API" -key "$KEY" -rate "${RATE:-50}" -duration 20s -users 200 -channels push,inapp -verify 300 -verify-timeout 90s
}

cmd_ws() { exec go run ./cmd/wsclient -user "$USER_ID" -key "$KEY"; }

cmd_help() {
  cat <<'TXT'
usage: scripts/demo.sh <step> [args]
  seed          templates and demo users (run once first)
  send          one notification to push, email and in-app, with delivery status and the email in Mailpit
  idem          same Idempotency-Key twice, one notification
  schedule [s]  send_at in s seconds (default 20)
  quiet         marketing message held until quiet hours end (about 1 minute)
  badtoken      provider rejects a token, status FAILED invalid token
  outage        provider down, retries, DLQ, recovery, replay (start the stack with RETRY_TIERS=1s,2s,3s,4s)
  campaign      bulk campaign of 100 recipients (needs the S3 container)
  load          load test with delivery verification (RATE=50 by default; see docs/defense.md before raising it)
  ws            print live in-app messages for the demo user (run in a second terminal)
TXT
}

case "${1:-help}" in
  seed) cmd_seed ;; send) cmd_send ;; idem) cmd_idem ;; schedule) shift; cmd_schedule "$@" ;;
  quiet) cmd_quiet ;; badtoken) cmd_badtoken ;; outage) cmd_outage ;; campaign) cmd_campaign ;;
  load) cmd_load ;; ws) cmd_ws ;; help|*) cmd_help ;;
esac

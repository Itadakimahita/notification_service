#!/usr/bin/env bash
# Chaos scenarios against a running local stack; each ends with a delivery verification (no sampled message may stay unaccounted).
# Requires: api, relay, router, worker --channel push, status, mockprovider running with RETRY_TIERS=1s,2s,3s,4s, and docker compose infra up.
set -u
cd "$(dirname "$0")/.."
API=${API_URL:-http://localhost:8081}
MOCK=${MOCK_URL:-http://localhost:9200}
RATE=${RATE:-100}
DUR=${DUR:-20s}

mock_pushes() { curl -s localhost:9105/metrics | awk '/^mockprovider_requests_total\{code="OK",kind="push"\}/{print $2}'; }

run_load() {
  bin/loadgen -url "$API" -rate "$RATE" -duration "$DUR" -users 200 -channels push -verify 500 -verify-timeout 150s > "/tmp/chaos-$1.out" 2>&1 &
  LOADPID=$!
}

report() {
  echo "---- $1"
  grep -E 'target=|latency|DELIVERED|FAILED|terminal=' "/tmp/chaos-$2.out" | sed 's/^/  /'
}

scenario_provider() {
  echo "[A] provider degraded: 50% errors + 2s latency during load"
  curl -s -XPOST "$MOCK/admin/config" -d '{"error_rate":0.5,"latency_ms":2000}' >/dev/null
  run_load provider
  sleep 22
  curl -s -XPOST "$MOCK/admin/config" -d '{"error_rate":0,"latency_ms":50}' >/dev/null
  wait $LOADPID; report "provider degraded" provider
}

scenario_worker() {
  echo "[B] worker crash: kill -9 push worker mid-load, restart 5s later"
  before=$(mock_pushes)
  run_load worker
  sleep 8; pkill -9 -f 'bin/worker --channel push'
  sleep 5; (ADMIN_ADDR=:9104 nohup bin/worker --channel push > /tmp/w-push.log 2>&1 &)
  wait $LOADPID; report "worker crash" worker
  echo "  provider sends during scenario: $(( $(mock_pushes) - before ))"
}

scenario_kafka() {
  echo "[C] kafka broker down: stop kafka-3 for 15s mid-load"
  run_load kafka
  sleep 5; docker stop notif-kafka-3-1 >/dev/null
  sleep 15; docker start notif-kafka-3-1 >/dev/null
  wait $LOADPID; report "kafka broker down" kafka
}

scenario_provider
scenario_worker
scenario_kafka

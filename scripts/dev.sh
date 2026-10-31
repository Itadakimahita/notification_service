#!/usr/bin/env bash
# Starts or stops every service on the host against the docker compose infrastructure.
set -euo pipefail
cd "$(dirname "$0")/.."

export KAFKA_BROKERS=${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}
export DYNAMO_ENDPOINT=${DYNAMO_ENDPOINT:-http://localhost:8000}
export REDIS_ADDR=${REDIS_ADDR:-localhost:6380}
export S3_ENDPOINT=${S3_ENDPOINT:-http://localhost:9300}
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT-}

RUN=.run
SERVICES="api relay router scheduler status inapp campaign lagexporter mockprovider worker"

start() {
  mkdir -p "$RUN" bin
  for s in $SERVICES; do go build -o "bin/$s" "./cmd/$s"; done
  launch() { # name, env..., command
    local name=$1; shift
    if [ -f "$RUN/$name.pid" ] && kill -0 "$(cat "$RUN/$name.pid")" 2>/dev/null; then echo "$name already running"; return; fi
    env "$@" > "$RUN/$name.log" 2>&1 &
    echo $! > "$RUN/$name.pid"
  }
  launch mockprovider MOCK_WEBHOOK_URL=http://localhost:8082/v1/webhooks/mock bin/mockprovider
  for s in api relay router scheduler status inapp campaign lagexporter; do launch "$s" bin/"$s"; done
  launch worker-push ADMIN_ADDR=:9104 bin/worker --channel push
  launch worker-email ADMIN_ADDR=:9114 bin/worker --channel email
  launch worker-sms ADMIN_ADDR=:9134 bin/worker --channel sms
  launch worker-inapp ADMIN_ADDR=:9124 bin/worker --channel inapp
  echo "started; logs in $RUN/, API on http://localhost:8081 (key demo-key)"
}

stop() {
  [ -d "$RUN" ] || { echo "nothing to stop"; return; }
  for f in "$RUN"/*.pid; do [ -e "$f" ] && kill -TERM "$(cat "$f")" 2>/dev/null || true; done
  for _ in $(seq 1 20); do
    alive=0
    for f in "$RUN"/*.pid; do [ -e "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null && alive=1; done
    [ "$alive" = 0 ] && break
    sleep 0.5
  done
  for f in "$RUN"/*.pid; do [ -e "$f" ] && { kill -KILL "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; }; done
  echo "stopped"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  *) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac

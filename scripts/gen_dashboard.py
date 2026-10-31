#!/usr/bin/env python3
"""Generates deploy/grafana/dashboards/notification-platform.json.

Run: python3 scripts/gen_dashboard.py
Every expression only uses metrics that the services expose (see docs/observability.md).
"""
import json
import os

DS = {"type": "prometheus", "uid": "prometheus"}
RI = "$__rate_interval"

panels = []
state = {"y": 0, "x": 0, "row_h": 0, "id": 0}


def _next_id():
    state["id"] += 1
    return state["id"]


def _place(w, h):
    if state["x"] + w > 24:
        state["y"] += state["row_h"]
        state["x"], state["row_h"] = 0, 0
    pos = {"x": state["x"], "y": state["y"], "w": w, "h": h}
    state["x"] += w
    state["row_h"] = max(state["row_h"], h)
    return pos


def row(title):
    state["y"] += state["row_h"]
    state["x"], state["row_h"] = 0, 0
    panels.append({"type": "row", "title": title, "id": _next_id(), "collapsed": False,
                   "gridPos": {"x": 0, "y": state["y"], "w": 24, "h": 1}, "panels": []})
    state["y"] += 1


def targets(exprs):
    return [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": DS} for i, (e, l) in enumerate(exprs)]


def ts(title, exprs, unit="short", w=12, h=8, desc="", line=None, stack=False):
    defaults = {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 10, "showPoints": "never"}}
    if stack:
        defaults["custom"]["stacking"] = {"mode": "normal"}
        defaults["custom"]["fillOpacity"] = 40
    if line is not None:
        defaults["thresholds"] = {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": line}]}
        defaults["custom"]["thresholdsStyle"] = {"mode": "line"}
    panels.append({"type": "timeseries", "title": title, "id": _next_id(), "description": desc, "datasource": DS,
                   "gridPos": _place(w, h), "fieldConfig": {"defaults": defaults, "overrides": []},
                   "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]},
                               "tooltip": {"mode": "multi"}},
                   "targets": targets(exprs)})


def stat(title, expr, unit="short", w=4, h=4, steps=None, desc=""):
    steps = steps or [{"color": "green", "value": None}]
    panels.append({"type": "stat", "title": title, "id": _next_id(), "description": desc, "datasource": DS,
                   "gridPos": _place(w, h),
                   "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps}}, "overrides": []},
                   "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "area"},
                   "targets": targets([(expr, "")])})


def q(p, metric, sel="", by=""):
    """histogram_quantile over a histogram, optionally split by labels."""
    grp = f"le, {by}" if by else "le"
    return f"histogram_quantile({p}, sum by ({grp}) (rate({metric}_bucket{{{sel}}}[{RI}])))"


def pct(metric, sel="", by="", legend=""):
    """p50/p95/p99 series for a histogram."""
    suffix = f" {legend}" if legend else ""
    return [(q(0.50, metric, sel, by), f"p50{suffix}"), (q(0.95, metric, sel, by), f"p95{suffix}"), (q(0.99, metric, sel, by), f"p99{suffix}")]


HTTP = "http_request_duration_seconds"
ENQUEUE = 'job="api",route="/v1/notifications",method="POST"'

# ---------------------------------------------------------------- overview
row("Overview")
stat("Accept rate", f'sum(rate({HTTP}_count{{{ENQUEUE},status="202"}}[{RI}]))', "reqps",
     desc="Notifications accepted per second by the API.")
stat("Enqueue p95", q(0.95, HTTP, ENQUEUE), "s",
     steps=[{"color": "green", "value": None}, {"color": "orange", "value": 0.05}, {"color": "red", "value": 0.1}],
     desc="Target: below 100 ms.")
stat("API 5xx ratio", f'sum(rate({HTTP}_count{{job="api",status=~"5.."}}[{RI}])) / sum(rate({HTTP}_count{{job="api"}}[{RI}]))', "percentunit",
     steps=[{"color": "green", "value": None}, {"color": "red", "value": 0.01}])
stat("Consumer lag (all groups)", 'sum(kafka_consumer_lag{group!~".*retry.*"})', "short",
     steps=[{"color": "green", "value": None}, {"color": "orange", "value": 10000}, {"color": "red", "value": 100000}],
     desc="Messages waiting in Kafka across the main groups.")
stat("Dead-letter depth", "sum(notification_dlq_depth)", "short",
     steps=[{"color": "green", "value": None}, {"color": "orange", "value": 1}],
     desc="Messages retained in all DLQ topics. Replay does not delete, so watch growth.")
stat("Services up", "sum(up)", "short", desc="Scrape targets that answered.")

# ---------------------------------------------------------------- api
row("API (accept path)")
ts("Request rate by route and status", [(f'sum by (route, status) (rate({HTTP}_count{{job="api"}}[{RI}]))', "{{route}} {{status}}")], "reqps")
ts("Enqueue latency p50 / p95 / p99 (POST /v1/notifications)", pct(HTTP, ENQUEUE), "s",
   desc="Red line is the 100 ms p95 target.", line=0.1)
ts("p95 latency by route", [(q(0.95, HTTP, 'job="api"', "route"), "{{route}}")], "s")
ts("p99 latency by route", [(q(0.99, HTTP, 'job="api"', "route"), "{{route}}")], "s")
ts("Errors by status code (4xx and 5xx)", [(f'sum by (status) (rate({HTTP}_count{{job="api",status=~"[45].."}}[{RI}]))', "{{status}}")], "reqps")
ts("In-app inbox API latency p50 / p95 / p99", pct(HTTP, 'job="api",route=~"/v1/users/{id}/inbox.*"'), "s")

# ---------------------------------------------------------------- relay & scheduler
row("Outbox relay and scheduler")
ts("Relay published to Kafka", [(f"sum(rate(relay_published_total[{RI}]))", "published/s")], "ops",
   desc="DynamoDB Streams to notifications.requested.")
ts("Owned shards (relay and scheduler)", [("max(relay_owned_shards)", "relay stream shards"), ("max(scheduler_owned_shards)", "scheduler schedule shards")], "short",
   desc="Zero means nothing is publishing or firing. The alerts watch this.")
ts("Scheduler re-emitted due requests", [(f"sum(rate(scheduler_emitted_total[{RI}]))", "emitted/s")], "ops")
ts("Accepted vs published (relay keeps up?)", [(f'sum(rate({HTTP}_count{{{ENQUEUE},status="202"}}[{RI}]))', "accepted/s"), (f"sum(rate(relay_published_total[{RI}]))", "published/s")], "ops")

# ---------------------------------------------------------------- router
row("Router")
ts("Routing time p50 / p95 / p99", pct("router_route_seconds"), "s",
   desc="Time to apply policy, render and fan out one request. Includes retried infrastructure calls.")
ts("Processed by result", [(f"sum by (result) (rate(router_processed_total[{RI}]))", "{{result}}")], "ops",
   desc="ok, retry (infrastructure failure, retried in place) and poison (sent to the router DLQ).", stack=True)

# ---------------------------------------------------------------- workers
row("Workers and providers")
ts("Outcomes by channel and result", [(f"sum by (channel, result) (rate(worker_send_total[{RI}]))", "{{channel}} {{result}}")], "ops")
ts("Success ratio by channel", [(f'sum by (channel) (rate(worker_send_total{{result="sent"}}[{RI}])) / sum by (channel) (rate(worker_send_total{{result!~"duplicate|in_flight|expired"}}[{RI}]))', "{{channel}}")], "percentunit",
   desc="Share of send attempts that succeeded, ignoring duplicates, in-flight claims and expired messages.")
ts("Provider call latency p50 (by channel)", [(q(0.50, "worker_send_seconds", "", "channel"), "{{channel}}")], "s")
ts("Provider call latency p95 (by channel)", [(q(0.95, "worker_send_seconds", "", "channel"), "{{channel}}")], "s")
ts("Provider call latency p99 (by channel)", [(q(0.99, "worker_send_seconds", "", "channel"), "{{channel}}")], "s")
ts("Retries, dead letters and permanent failures", [
    (f'sum by (channel) (rate(worker_send_total{{result="retry"}}[{RI}]))', "retry {{channel}}"),
    (f'sum by (channel) (rate(worker_send_total{{result="dead"}}[{RI}]))', "dead {{channel}}"),
    (f'sum by (channel) (rate(worker_send_total{{result=~"permanent|invalid_token"}}[{RI}]))', "permanent {{channel}}")], "ops")
ts("Circuit breaker, rate limiting and in-flight conflicts", [
    (f'sum by (channel) (rate(worker_send_total{{result="breaker_open"}}[{RI}]))', "breaker open {{channel}}"),
    (f'sum by (channel) (rate(worker_send_total{{result="rate_limited"}}[{RI}]))', "rate limited {{channel}}"),
    (f'sum by (channel) (rate(worker_send_total{{result="in_flight"}}[{RI}]))', "in flight {{channel}}"),
    (f'sum by (channel) (rate(worker_send_total{{result="duplicate"}}[{RI}]))', "duplicate skipped {{channel}}")], "ops")

# ---------------------------------------------------------------- status
row("Status tracking and webhooks")
ts("Status events vs DynamoDB updates", [(f"sum(rate(status_events_total[{RI}]))", "events consumed/s"), (f"sum(rate(status_updates_total[{RI}]))", "updates written/s")], "ops",
   desc="The gap is saved by coalescing. A growing lag next to this panel means the status store is the bottleneck.")
ts("Coalescing ratio", [(f"1 - sum(rate(status_updates_total[{RI}])) / sum(rate(status_events_total[{RI}]))", "writes saved")], "percentunit")
ts("Webhook handler latency p50 / p95 / p99", pct(HTTP, 'job="status",route="/v1/webhooks/{provider}"'), "s")
ts("Webhook requests by status", [(f'sum by (status) (rate({HTTP}_count{{job="status",route="/v1/webhooks/{{provider}}"}}[{RI}]))', "{{status}}")], "reqps")
ts("Status topic consumer lag", [('max(kafka_consumer_lag{group="status"})', "status group")], "short")

# ---------------------------------------------------------------- kafka
row("Kafka")
ts("Consumer lag by group and topic", [('max by (group, topic) (kafka_consumer_lag)', "{{group}} / {{topic}}")], "short")
ts("Dead-letter depth by topic", [("notification_dlq_depth", "{{topic}}")], "short")
ts("Lag growth rate (positive means falling behind)", [('sum by (group) (deriv(kafka_consumer_lag{group!~".*retry.*"}[5m]))', "{{group}}")], "ops")
ts("Retry backlog", [('sum by (group) (kafka_consumer_lag{group=~".*retry.*"})', "{{group}}")], "short",
   desc="Messages waiting in retry tiers. Normal under provider trouble, a problem if it never drains.")

# ---------------------------------------------------------------- in-app
row("In-app gateway")
ts("Open WebSocket connections", [("sum(inapp_connections)", "connections")], "short")
ts("Pushed vs dropped messages", [(f"sum(rate(inapp_pushed_total[{RI}]))", "pushed/s"), (f"sum(rate(inapp_dropped_total[{RI}]))", "dropped (slow client)/s")], "ops")

# ---------------------------------------------------------------- campaigns & mock
row("Campaigns and mock provider")
ts("Campaign recipients accepted", [(f"sum(rate(campaign_emitted_total[{RI}]))", "emitted/s")], "ops")
ts("Mock provider requests", [(f"sum by (kind, code) (rate(mockprovider_requests_total[{RI}]))", "{{kind}} {{code}}")], "reqps",
   desc="Only present in local runs.")

# ---------------------------------------------------------------- runtime
row("Runtime (all services)")
ts("Targets up", [("up", "{{job}} {{instance}}")], "short")
ts("Goroutines", [("sum by (job) (go_goroutines)", "{{job}}")], "short")
ts("Resident memory", [("sum by (job) (process_resident_memory_bytes)", "{{job}}")], "bytes")
ts("CPU usage", [(f"sum by (job) (rate(process_cpu_seconds_total[{RI}]))", "{{job}}")], "percentunit",
   desc="1.0 equals one full core.")
ts("GC pause (max)", [("max by (job) (go_gc_duration_seconds{quantile=\"1\"})", "{{job}}")], "s")
ts("Open file descriptors", [("sum by (job) (process_open_fds)", "{{job}}")], "short")

dash = {
    "uid": "notification-platform",
    "title": "Notification Platform",
    "tags": ["notifications"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 2,
    "refresh": "10s",
    "time": {"from": "now-30m", "to": "now"},
    "panels": panels,
}

out = os.path.join(os.path.dirname(__file__), "..", "deploy", "grafana", "dashboards", "notification-platform.json")
with open(out, "w") as f:
    json.dump(dash, f, indent=1)
print(f"{sum(1 for p in panels if p['type'] != 'row')} panels in {sum(1 for p in panels if p['type'] == 'row')} rows -> {os.path.normpath(out)}")

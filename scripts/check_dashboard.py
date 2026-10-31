#!/usr/bin/env python3
"""Evaluates every panel expression of the dashboard against a running Prometheus.

Exit code 1 when any expression is invalid. Expressions that return no data are listed
because they depend on traffic (for example the circuit breaker) and are not errors.
Run: python3 scripts/check_dashboard.py [http://localhost:9090]
"""
import json
import os
import sys
import urllib.parse
import urllib.request

prom = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:9090"
path = os.path.join(os.path.dirname(__file__), "..", "deploy", "grafana", "dashboards", "notification-platform.json")
dash = json.load(open(path))

bad, empty, ok = [], [], 0
for p in dash["panels"]:
    for t in p.get("targets", []):
        expr = t["expr"].replace("$__rate_interval", "1m")
        url = f"{prom}/api/v1/query?query={urllib.parse.quote(expr)}"
        try:
            body = json.load(urllib.request.urlopen(url, timeout=15))
        except Exception as e:  # noqa: BLE001
            bad.append((p["title"], f"request failed: {e}"))
            continue
        if body.get("status") != "success":
            bad.append((p["title"], body.get("error", "unknown error")))
        elif not body["data"]["result"]:
            empty.append(p["title"])
        else:
            ok += 1

print(f"expressions with data: {ok}")
if empty:
    print(f"expressions returning no data ({len(empty)}):")
    for title in sorted(set(empty)):
        print("  -", title)
if bad:
    print(f"INVALID expressions ({len(bad)}):")
    for title, err in bad:
        print("  -", title, "->", err)
    sys.exit(1)
print("all expressions are valid")

#!/usr/bin/env python3
"""Bulk-index synthetic documents into an Elasticsearch data stream.

Data stream name format: {type}-{dataset}-{namespace}
Examples: logs-nginx-prod, metrics-postgresql-acc
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

HOSTNAMES = ("edge-01", "edge-02", "web-01", "web-02", "db-01", "db-02")
METHODS = ("GET", "POST", "PUT", "HEAD")
PATHS = ("/", "/api/health", "/api/users", "/static/app.js", "/login")
STATUSES = ((200, 0.85), (404, 0.08), (500, 0.07))
PG_DATABASES = ("app", "billing", "analytics", "auth")
PG_STATEMENTS = (
    "SELECT * FROM users WHERE id = $1",
    "INSERT INTO orders (user_id, total) VALUES ($1, $2)",
    "UPDATE sessions SET last_seen = now() WHERE id = $1",
    "SELECT count(*) FROM events WHERE ts > now() - interval '1 hour'",
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simulate load into a data stream")
    p.add_argument("--data-stream", required=True, help="e.g. logs-nginx-prod")
    p.add_argument("--host", default=os.environ.get("ES_HOST", "https://localhost:9200"))
    p.add_argument("--user", default=os.environ.get("ES_USER", "elastic"))
    p.add_argument("--password", default=os.environ.get("ES_PASSWORD", "changeme-elastic"))
    p.add_argument("--api-key", default=os.environ.get("ES_API_KEY"))
    p.add_argument("--count", type=int, default=500_000)
    p.add_argument("--batch-size", type=int, default=2_500)
    p.add_argument("--seed", type=int, default=11)
    p.add_argument("--pause-ms", type=int, default=25)
    p.add_argument("--max-retries", type=int, default=12)
    p.add_argument("--ca-cert", default=os.environ.get("REQUESTS_CA_BUNDLE"))
    p.add_argument("--days-back", type=float, default=1.0)
    return p.parse_args()


def parse_data_stream(name: str) -> tuple[str, str, str]:
    if name.count("-") < 2:
        raise SystemExit(f"Invalid data stream name: {name!r} (expected type-dataset-namespace)")
    stream_type, rest = name.split("-", 1)
    dataset, namespace = rest.rsplit("-", 1)
    if stream_type not in ("logs", "metrics", "traces"):
        raise SystemExit(f"Unsupported data stream type: {stream_type}")
    return stream_type, dataset, namespace


def auth_headers(args: argparse.Namespace) -> dict[str, str]:
    return {"Authorization": f"ApiKey {args.api_key}"} if args.api_key else {}


def auth_tuple(args: argparse.Namespace) -> tuple[str, str] | None:
    return None if args.api_key else (args.user, args.password)


def weighted_status(rng: random.Random) -> int:
    roll = rng.random()
    total = 0.0
    for status, weight in STATUSES:
        total += weight
        if roll <= total:
            return status
    return 200


def gen_log_doc(
    rng: random.Random,
    seq: int,
    dataset: str,
    namespace: str,
    time_start: datetime,
    time_end: datetime,
) -> dict:
    span = max(1.0, (time_end - time_start).total_seconds())
    ts = time_start + timedelta(seconds=rng.uniform(0, span))
    hostname = rng.choice(HOSTNAMES)
    method = rng.choice(METHODS)
    path = rng.choice(PATHS)
    status = weighted_status(rng)
    remote_ip = f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
    line = (
        f'{remote_ip} - - [{ts.strftime("%d/%b/%Y:%H:%M:%S +0000")}] '
        f'"{method} {path} HTTP/1.1" {status} {rng.randint(128, 4096)} "-" "curl/8.5.0"'
    )
    return {
        "@timestamp": ts.isoformat(),
        "message": line,
        "event": {"dataset": dataset, "module": dataset, "category": ["web"]},
        "data_stream": {"type": "logs", "dataset": dataset, "namespace": namespace},
        "host": {"hostname": hostname, "name": hostname},
        "http": {"request": {"method": method}, "response": {"status_code": status}},
        "nginx": {"access": {"remote_ip": remote_ip, "response_status": status}},
        "log": {"file": {"path": "/var/log/nginx/access.log"}},
        "labels": {"seq": seq},
    }


def gen_metrics_doc(
    rng: random.Random,
    seq: int,
    dataset: str,
    namespace: str,
    time_start: datetime,
    time_end: datetime,
) -> dict:
    span = max(1.0, (time_end - time_start).total_seconds())
    ts = time_start + timedelta(seconds=rng.uniform(0, span))
    hostname = rng.choice(HOSTNAMES)
    database = rng.choice(PG_DATABASES)
    return {
        "@timestamp": ts.isoformat(),
        "data_stream": {"type": "metrics", "dataset": dataset, "namespace": namespace},
        "event": {"dataset": dataset, "module": dataset, "category": ["database"]},
        "host": {"hostname": hostname, "name": hostname},
        "service": {"type": dataset, "address": hostname},
        "postgresql": {
            "database": {
                "name": database,
                "oid": rng.randint(10000, 50000),
                "transactions": {
                    "commit": rng.randint(100, 50000),
                    "rollback": rng.randint(0, 500),
                },
                "blocks": {"hit": rng.randint(1000, 500000), "read": rng.randint(0, 5000)},
            },
            "activity": {
                "state": rng.choice(("active", "idle", "idle in transaction")),
                "query": rng.choice(PG_STATEMENTS),
                "backend_type": rng.choice(("client backend", "autovacuum worker")),
            },
            "statement": {
                "calls": rng.randint(1, 5000),
                "rows": rng.randint(0, 10000),
                "total_time": {"ms": round(rng.uniform(0.1, 250.0), 3)},
            },
        },
        "metricset": {"name": "activity", "period": 10000},
        "labels": {"seq": seq},
    }


def bulk_batch(data_stream: str, docs: list[dict]) -> str:
    lines = []
    for doc in docs:
        lines.append(json.dumps({"create": {"_index": data_stream}}))
        lines.append(json.dumps(doc))
    return "\n".join(lines) + "\n"


def post_bulk(session: requests.Session, args: argparse.Namespace, body: str) -> dict:
    url = f"{args.host.rstrip('/')}/_bulk"
    headers = {"Content-Type": "application/x-ndjson"}
    last_err: Exception | None = None
    for attempt in range(1, args.max_retries + 1):
        try:
            r = session.post(
                url,
                data=body,
                headers=headers,
                auth=auth_tuple(args),
                verify=args.ca_cert if args.ca_cert else True,
                timeout=180,
            )
            if r.ok:
                return r.json()
            last_err = RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
        except requests.RequestException as exc:
            last_err = exc
        wait = min(30, 2 ** attempt)
        print(f"  bulk retry {attempt}/{args.max_retries} in {wait}s ({last_err})", flush=True)
        time.sleep(wait)
    raise RuntimeError(f"Bulk failed after {args.max_retries} retries: {last_err}")


def main() -> int:
    args = parse_args()
    stream_type, dataset, namespace = parse_data_stream(args.data_stream)
    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc)
    time_start = now - timedelta(days=args.days_back)
    time_end = now

    session = requests.Session()
    session.headers.update(auth_headers(args))
    verify = args.ca_cert if args.ca_cert else True

    health = session.get(
        f"{args.host.rstrip('/')}/_cluster/health",
        auth=auth_tuple(args),
        verify=verify,
        timeout=30,
    )
    health.raise_for_status()
    cluster = health.json().get("cluster_name", "?")

    gen = gen_log_doc if stream_type == "logs" else gen_metrics_doc
    if stream_type == "traces":
        print("traces not implemented in this simulator; use logs or metrics", file=sys.stderr)
        return 1

    print(f"Target: {args.host} (cluster: {cluster})")
    print(f"Data stream: {args.data_stream} ({args.count:,} docs, batch={args.batch_size:,})")

    sent = 0
    errors = 0
    t0 = time.perf_counter()

    while sent < args.count:
        n = min(args.batch_size, args.count - sent)
        batch_docs = [
            gen(rng, sent + i + 1, dataset, namespace, time_start, time_end)
            for i in range(n)
        ]
        result = post_bulk(session, args, bulk_batch(args.data_stream, batch_docs))
        if result.get("errors"):
            for item in result.get("items", []):
                action = item.get("create") or item.get("index") or {}
                if "error" in action:
                    errors += 1
                    if errors <= 5:
                        print(f"  index error: {action['error']}", file=sys.stderr)

        sent += n
        elapsed = time.perf_counter() - t0
        rate = sent / elapsed if elapsed else 0
        print(
            f"  indexed {sent:>10,} / {args.count:,}  ({rate:,.0f} docs/s, errors={errors})",
            flush=True,
        )
        if args.pause_ms > 0 and sent < args.count:
            time.sleep(args.pause_ms / 1000)

    elapsed = time.perf_counter() - t0
    print(f"\nDone: {sent:,} documents in {elapsed:.1f}s ({sent / elapsed:,.0f} docs/s)")
    print(f"Verify: GET {args.host}/{args.data_stream}/_count")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Bulk-index dummy nginx access logs into an Elasticsearch data stream."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

DATA_STREAM = "logs-nginx.access-tst"
DATASET = "nginx.access"
NAMESPACE = "tst"

HOSTNAMES = ("edge-01", "edge-02", "web-01", "web-02", "lb-01", "cdn-01")
METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")
PATHS = (
    "/",
    "/index.html",
    "/api/v1/health",
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v2/search",
    "/static/css/main.css",
    "/static/js/app.js",
    "/static/img/logo.png",
    "/login",
    "/logout",
    "/dashboard",
    "/favicon.ico",
    "/robots.txt",
    "/.well-known/health",
    "/nginx_status",
)
STATUSES = (
    (200, 0.62),
    (201, 0.03),
    (204, 0.02),
    (301, 0.05),
    (304, 0.08),
    (400, 0.04),
    (401, 0.03),
    (403, 0.02),
    (404, 0.07),
    (429, 0.01),
    (500, 0.02),
    (502, 0.005),
    (503, 0.005),
)
USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "curl/8.5.0",
    "PostmanRuntime/7.39.0",
    "ELB-HealthChecker/2.0",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
    "python-requests/2.31.0",
    "kube-probe/1.29",
)
REFERRERS = (
    "-",
    "https://www.google.com/",
    "https://example.com/dashboard",
    "https://intranet.corp/login",
    "-",
    "-",
)
HTTP_VERSIONS = ("HTTP/1.1", "HTTP/2.0", "HTTP/1.0")
USERS = ("-", "admin", "appuser", "deploy", "-", "-", "-")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simulate nginx access log indexing load")
    p.add_argument("--host", default=os.environ.get("ES_HOST", "https://localhost:9201"))
    p.add_argument("--user", default=os.environ.get("ES_USER", "elastic"))
    p.add_argument("--password", default=os.environ.get("ES_PASSWORD", "changeme-elastic"))
    p.add_argument("--api-key", default=os.environ.get("ES_API_KEY"))
    p.add_argument("--count", type=int, default=5_000_000)
    p.add_argument("--batch-size", type=int, default=2_500)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--start-seq", type=int, default=0, help="Minimum sequence offset (resume uses index count when --target-total is set)")
    p.add_argument("--target-total", type=int, default=None, help="Index until data stream reaches this count")
    p.add_argument("--pause-ms", type=int, default=50, help="Pause between bulk batches")
    p.add_argument("--max-retries", type=int, default=12)
    p.add_argument("--ca-cert", default=os.environ.get("REQUESTS_CA_BUNDLE"))
    p.add_argument("--days-back", type=float, default=1.0, help="Legacy: spread timestamps over the last N days ending now")
    p.add_argument("--days-ago-start", type=float, default=None, help="Oldest @timestamp boundary, days ago (e.g. 3 = now-3d)")
    p.add_argument("--days-ago-end", type=float, default=None, help="Newest @timestamp boundary, days ago (e.g. 1 = now-1d)")
    return p.parse_args()


def auth_headers(args: argparse.Namespace) -> dict[str, str]:
    if args.api_key:
        return {"Authorization": f"ApiKey {args.api_key}"}
    return {}


def auth_tuple(args: argparse.Namespace) -> tuple[str, str] | None:
    if args.api_key:
        return None
    return (args.user, args.password)


def weighted_status(rng: random.Random) -> int:
    roll = rng.random()
    cumulative = 0.0
    for status, weight in STATUSES:
        cumulative += weight
        if roll <= cumulative:
            return status
    return 200


def nginx_time(ts: datetime) -> str:
    return ts.strftime("%d/%b/%Y:%H:%M:%S %z").replace("+0000", "+0000")


def gen_nginx_line(
    rng: random.Random, seq: int, time_start: datetime, time_end: datetime
) -> tuple[str, dict]:
    hostname = rng.choice(HOSTNAMES)
    remote_ip = f"{rng.randint(1, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
    method = rng.choice(METHODS)
    path = rng.choice(PATHS)
    if rng.random() < 0.15:
        path = f"{path}?q={rng.randint(1, 99999)}&page={rng.randint(1, 50)}"
    if rng.random() < 0.08:
        path = path.rstrip("/") + f"/{rng.randint(1, 999999)}"

    status = weighted_status(rng)
    body_bytes = 0 if method == "HEAD" else rng.randint(0, 2_500_000)
    if status in (204, 304):
        body_bytes = 0
    elif status >= 400:
        body_bytes = rng.randint(150, 4096)

    http_ver = rng.choice(HTTP_VERSIONS)
    user = rng.choice(USERS)
    ident = "-"
    referrer = rng.choice(REFERRERS)
    agent = rng.choice(USER_AGENTS)
    request_time = round(rng.uniform(0.001, 2.5), 3)
    upstream_time = round(max(0.0, request_time - rng.uniform(0, 0.05)), 3)

    span_sec = max(1.0, (time_end - time_start).total_seconds())
    offset = rng.uniform(0, span_sec)
    ts = time_start + timedelta(seconds=offset)
    ts_iso = ts.isoformat()
    ts_nginx = nginx_time(ts.astimezone(timezone.utc))

    # Combined log format (nginx default)
    line = (
        f'{remote_ip} {ident} {user} [{ts_nginx}] '
        f'"{method} {path} {http_ver}" {status} {body_bytes} '
        f'"{referrer}" "{agent}"'
    )

    # Optional upstream fields appended like many real nginx configs
    if rng.random() < 0.7:
        upstream = rng.choice(("127.0.0.1:8080", "10.0.1.10:8080", "10.0.1.11:8080", "unix:/run/app.sock"))
        line += f' rt={request_time} uct="{upstream_time}" uht="{upstream_time}" urt="{upstream_time}" '
        line += f'upstream="{upstream}" host="{hostname}" request_id="{seq:012x}"'

    doc = {
        "@timestamp": ts_iso,
        "message": line,
        "event": {
            "dataset": DATASET,
            "module": "nginx",
            "category": ["web"],
            "type": ["access"],
        },
        "data_stream": {
            "type": "logs",
            "dataset": DATASET,
            "namespace": NAMESPACE,
        },
        "host": {"hostname": hostname, "name": hostname},
        "source": {"ip": remote_ip},
        "url": {"path": path.split("?", 1)[0], "query": path.split("?", 1)[1] if "?" in path else None},
        "http": {
            "request": {"method": method, "referrer": None if referrer == "-" else referrer},
            "response": {
                "status_code": status,
                "body": {"bytes": body_bytes},
            },
            "version": http_ver.removeprefix("HTTP/"),
        },
        "user_agent": {"original": agent},
        "nginx": {
            "access": {
                "remote_ip": remote_ip,
                "user_name": None if user == "-" else user,
                "response_status": status,
                "body_sent": {"bytes": body_bytes},
                "request_time": request_time,
                "upstream_response_time": upstream_time,
            }
        },
        "log": {"file": {"path": "/var/log/nginx/access.log"}},
    }
    if doc["url"]["query"] is None:
        del doc["url"]["query"]
    return line, doc


def bulk_batch(docs: list[dict]) -> str:
    lines: list[str] = []
    for doc in docs:
        lines.append(json.dumps({"create": {"_index": DATA_STREAM}}))
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


def resolve_time_window(args: argparse.Namespace) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    if args.days_ago_start is not None or args.days_ago_end is not None:
        if args.days_ago_start is None or args.days_ago_end is None:
            raise SystemExit("Both --days-ago-start and --days-ago-end are required together.")
        if args.days_ago_start <= args.days_ago_end:
            raise SystemExit("--days-ago-start must be greater than --days-ago-end (e.g. 3 and 1).")
        return now - timedelta(days=args.days_ago_start), now - timedelta(days=args.days_ago_end)
    return now - timedelta(days=args.days_back), now


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)
    time_start, time_end = resolve_time_window(args)

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

    if args.target_total is not None:
        count_r = session.get(
            f"{args.host.rstrip('/')}/{DATA_STREAM}/_count",
            auth=auth_tuple(args),
            verify=verify,
            timeout=30,
        )
        existing = count_r.json().get("count", 0) if count_r.ok else 0
        args.start_seq = max(args.start_seq, existing)
        args.count = max(0, args.target_total - existing)
        if args.count == 0:
            print(f"Target already reached: {existing:,} documents in {DATA_STREAM}")
            return 0
    elif args.start_seq == 0:
        count_r = session.get(
            f"{args.host.rstrip('/')}/{DATA_STREAM}/_count",
            auth=auth_tuple(args),
            verify=verify,
            timeout=30,
        )
        if count_r.ok:
            args.start_seq = count_r.json().get("count", 0)

    print(f"Target: {args.host} (cluster: {cluster})")
    print(f"Data stream: {DATA_STREAM} ({args.count:,} documents, batch={args.batch_size:,})")
    print(f"Timestamp window: {time_start.isoformat()} -> {time_end.isoformat()}")

    sent = 0
    errors = 0
    t0 = time.perf_counter()
    seq_base = args.start_seq

    while sent < args.count:
        n = min(args.batch_size, args.count - sent)
        batch_docs = [
            gen_nginx_line(rng, seq_base + sent + i + 1, time_start, time_end)[1]
            for i in range(n)
        ]

        result = post_bulk(session, args, bulk_batch(batch_docs))
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
            f"  indexed {seq_base + sent:>10,} total ({sent:,} this run) / {args.count:,}  "
            f"({rate:,.0f} docs/s, errors={errors})",
            flush=True,
        )
        if args.pause_ms > 0 and sent < args.count:
            time.sleep(args.pause_ms / 1000)

    elapsed = time.perf_counter() - t0
    print(f"\nDone: {sent:,} documents in {elapsed:.1f}s ({sent / elapsed:,.0f} docs/s)")
    print(f"Verify: GET {args.host}/{DATA_STREAM}/_count")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

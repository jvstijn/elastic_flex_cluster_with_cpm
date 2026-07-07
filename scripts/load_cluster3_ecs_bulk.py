#!/usr/bin/env python3
"""Parallel ECS bulk load into six data streams on cluster03 (CPM discovery test).

Each stream gets --count documents (default 500_000) with at least
@timestamp, host.name, and message, plus stream-specific ECS fields.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

HOSTS = ("cl03-app-01", "cl03-app-02", "cl03-api-01", "cl03-worker-01", "cl03-edge-01", "cl03-db-01")
SERVICES = ("gateway", "auth", "worker", "firewall", "kube-apiserver", "cache")

STREAMS: list[tuple[str, str, int]] = [
    ("logs-app-gateway-prd.03", "gateway", 101),
    ("logs-api-auth-prd.03", "auth", 102),
    ("logs-worker-jobs-prd.03", "worker", 103),
    ("logs-firewall-deny-prd.03", "firewall", 104),
    ("logs-k8s-audit-prd.03", "kube-apiserver", 105),
    ("logs-cache-miss-prd.03", "cache", 106),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bulk ECS load test on cluster03")
    p.add_argument("--host", default=os.environ.get("ES_HOST", "https://cluster03.kaposi.net"))
    p.add_argument("--user", default=os.environ.get("ES_USER", "elastic"))
    p.add_argument("--password", default=os.environ.get("ES_PASSWORD", ""))
    p.add_argument("--count", type=int, default=500_000)
    p.add_argument("--batch-size", type=int, default=5_000)
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--pause-ms", type=int, default=0)
    p.add_argument("--days-back", type=float, default=1.0)
    return p.parse_args()


def parse_data_stream(name: str) -> tuple[str, str, str]:
    stream_type, rest = name.split("-", 1)
    dataset, namespace = rest.rsplit("-", 1)
    return stream_type, dataset, namespace


def gen_doc(
    rng: random.Random,
    seq: int,
    data_stream: str,
    service: str,
    time_start: datetime,
    time_end: datetime,
) -> dict:
    stream_type, dataset, namespace = parse_data_stream(data_stream)
    span = max(1.0, (time_end - time_start).total_seconds())
    ts = time_start + timedelta(seconds=rng.uniform(0, span))
    hostname = rng.choice(HOSTS)
    level = rng.choices(("info", "warn", "error", "debug"), weights=(0.72, 0.15, 0.08, 0.05))[0]
    trace_id = f"{seq:012x}{rng.randint(0, 0xFFFFFF):06x}"
    user = rng.choice(("alice", "bob", "svc-batch", "anonymous", "admin"))

    if service == "gateway":
        method = rng.choice(("GET", "POST", "PUT"))
        path = rng.choice(("/api/v1/orders", "/api/v1/users", "/health", "/metrics"))
        status = rng.choices((200, 201, 400, 401, 500, 502), weights=(0.7, 0.1, 0.05, 0.05, 0.07, 0.03))[0]
        message = f"{method} {path} -> {status} latency={rng.randint(1, 800)}ms trace={trace_id}"
        extra = {
            "http": {"request": {"method": method}, "response": {"status_code": status}},
            "url": {"path": path},
            "service": {"name": "app-gateway", "type": "web"},
        }
    elif service == "auth":
        action = rng.choice(("login", "logout", "token_refresh", "mfa_challenge", "password_reset"))
        outcome = rng.choice(("success", "failure"))
        message = f"auth {action} user={user} outcome={outcome} ip=10.3.{rng.randint(0,255)}.{rng.randint(1,254)}"
        extra = {
            "event": {"action": action, "outcome": outcome, "category": ["authentication"]},
            "user": {"name": user},
            "source": {"ip": f"10.3.{rng.randint(0,255)}.{rng.randint(1,254)}"},
            "service": {"name": "api-auth", "type": "auth"},
        }
    elif service == "worker":
        job = rng.choice(("invoice-export", "email-dispatch", "report-rollup", "index-compact"))
        message = f"job {job} id={seq} status=completed duration={rng.randint(100, 9000)}ms items={rng.randint(1, 5000)}"
        extra = {
            "event": {"category": ["process"], "action": "job-complete"},
            "service": {"name": "worker-jobs", "type": "worker"},
            "labels": {"job_name": job},
        }
    elif service == "firewall":
        src = f"203.0.{rng.randint(1,254)}.{rng.randint(1,254)}"
        dst_port = rng.choice((22, 443, 8080, 9200))
        message = f"DROP tcp {src}:54321 -> 10.3.0.10:{dst_port} rule=deny-external-scan"
        extra = {
            "event": {"category": ["network"], "action": "deny", "type": ["denied"]},
            "source": {"ip": src, "port": rng.randint(1024, 65535)},
            "destination": {"ip": "10.3.0.10", "port": dst_port},
            "network": {"transport": "tcp", "direction": "inbound"},
        }
    elif service == "kube-apiserver":
        verb = rng.choice(("get", "list", "create", "update", "delete", "patch"))
        resource = rng.choice(("pods", "deployments", "secrets", "configmaps"))
        ns = rng.choice(("default", "prod", "monitoring"))
        message = f'audit {verb} {resource} namespace={ns} user=system:serviceaccount:prod:deployer response=201'
        extra = {
            "event": {"category": ["configuration"], "action": verb},
            "orchestrator": {"type": "kubernetes", "cluster": {"name": "cluster03"}},
            "kubernetes": {"namespace": ns, "resource": resource},
            "user": {"name": "system:serviceaccount:prod:deployer"},
        }
    else:
        key = f"session:{user}:{rng.randint(1, 99999)}"
        hit = rng.random() < 0.35
        message = f"cache {'HIT' if hit else 'MISS'} key={key} ttl={rng.randint(0, 3600)}s"
        extra = {
            "event": {"category": ["database"], "action": "cache-hit" if hit else "cache-miss"},
            "service": {"name": "redis-cache", "type": "cache"},
            "labels": {"cache_key_prefix": key.split(":")[0]},
        }

    doc = {
        "@timestamp": ts.isoformat(),
        "message": message,
        "log": {"level": level, "logger": f"{service}.loadtest"},
        "event": {
            "dataset": dataset,
            "module": service,
            "category": ["loadtest"],
            "kind": "event",
        },
        "data_stream": {"type": stream_type, "dataset": dataset, "namespace": namespace},
        "host": {"name": hostname, "hostname": hostname},
        "agent": {"type": "loadgen", "version": "1.0.0"},
        "ecs": {"version": "8.11.0"},
        "labels": {"load_test": "cluster03", "seq": seq, "trace_id": trace_id},
    }
    for key, value in extra.items():
        if key == "event" and isinstance(value, dict):
            doc["event"].update(value)
        else:
            doc[key] = value
    return doc


def bulk_body(data_stream: str, docs: list[dict]) -> str:
    lines: list[str] = []
    for doc in docs:
        lines.append(json.dumps({"create": {"_index": data_stream}}))
        lines.append(json.dumps(doc))
    return "\n".join(lines) + "\n"


def load_stream(
    host: str,
    user: str,
    password: str,
    data_stream: str,
    service: str,
    seed: int,
    count: int,
    batch_size: int,
    pause_ms: int,
    days_back: float,
) -> dict:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    time_start = now - timedelta(days=days_back)
    time_end = now
    session = requests.Session()
    session.auth = (user, password)
    url = f"{host.rstrip('/')}/_bulk"

    sent = 0
    errors = 0
    t0 = time.perf_counter()

    while sent < count:
        n = min(batch_size, count - sent)
        docs = [
            gen_doc(rng, sent + i + 1, data_stream, service, time_start, time_end)
            for i in range(n)
        ]
        r = session.post(url, data=bulk_body(data_stream, docs), headers={"Content-Type": "application/x-ndjson"}, timeout=180)
        r.raise_for_status()
        result = r.json()
        if result.get("errors"):
            for item in result.get("items", []):
                action = item.get("create") or {}
                if "error" in action:
                    errors += 1

        sent += n
        elapsed = time.perf_counter() - t0
        rate = sent / elapsed if elapsed else 0
        print(
            f"[{data_stream}] {sent:>9,}/{count:,} ({rate:,.0f} docs/s, errors={errors})",
            flush=True,
        )
        if pause_ms > 0 and sent < count:
            time.sleep(pause_ms / 1000)

    elapsed = time.perf_counter() - t0
    return {
        "data_stream": data_stream,
        "sent": sent,
        "errors": errors,
        "seconds": round(elapsed, 1),
        "rate": round(sent / elapsed if elapsed else 0),
    }


def main() -> int:
    args = parse_args()
    if not args.password:
        print("Set ES_PASSWORD or pass --password", file=sys.stderr)
        return 2

    health = requests.get(
        f"{args.host.rstrip('/')}/_cluster/health",
        auth=(args.user, args.password),
        timeout=30,
    )
    health.raise_for_status()
    cluster = health.json().get("cluster_name", "?")
    print(f"Target {args.host} cluster={cluster}")
    print(f"Loading {len(STREAMS)} streams x {args.count:,} docs (workers={args.workers})\n")

    tasks = STREAMS[: args.workers]
    results: list[dict] = []
    t0 = time.perf_counter()

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                load_stream,
                args.host,
                args.user,
                args.password,
                ds,
                svc,
                seed,
                args.count,
                args.batch_size,
                args.pause_ms,
                args.days_back,
            ): ds
            for ds, svc, seed in tasks
        }
        for fut in as_completed(futures):
            ds = futures[fut]
            try:
                results.append(fut.result())
            except Exception as exc:
                print(f"[{ds}] FAILED: {exc}", file=sys.stderr)
                return 1

    elapsed = time.perf_counter() - t0
    total = sum(r["sent"] for r in results)
    err = sum(r["errors"] for r in results)
    print(f"\nDone: {total:,} docs in {elapsed:.1f}s ({total/elapsed:,.0f} docs/s aggregate, errors={err})")
    for r in sorted(results, key=lambda x: x["data_stream"]):
        print(f"  {r['data_stream']}: {r['sent']:,} in {r['seconds']}s ({r['rate']:,}/s)")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

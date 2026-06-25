#!/usr/bin/env python3
"""Backfill .monitoring-es-8-* with synthetic Metricbeat-style ES monitoring history."""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

MONITORING_INDEX = ".monitoring-es-8-mb"

CLUSTERS = (
    {
        "id": "byNsBGQ5SHOHQDhjbivOag",
        "name": "central-cluster",
        "store_base": 400_000_000,
        "store_growth_per_day": 25_000_000,
        "heap_base": 58,
        "shards_base": 55,
        "queue_base": 2,
    },
    {
        "id": "RUEezM-NTT6VuyBo1iNh4w",
        "name": "remote-a",
        "store_base": 8_000_000_000,
        "store_growth_per_day": 400_000_000,
        "heap_base": 62,
        "shards_base": 40,
        "queue_base": 8,
    },
    {
        "id": "2fs2vbgpSf2JQt4yQ_bZAA",
        "name": "remote-b",
        "store_base": 120_000,
        "store_growth_per_day": 5_000,
        "heap_base": 52,
        "shards_base": 8,
        "queue_base": 1,
    },
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill stack monitoring history for CPM ML")
    p.add_argument("--host", default=os.environ.get("ES_HOST", "https://localhost:9200"))
    p.add_argument("--user", default=os.environ.get("ES_USER", "elastic"))
    p.add_argument("--password", default=os.environ.get("ES_PASSWORD", "changeme-elastic"))
    p.add_argument("--days", type=float, default=7.0)
    p.add_argument("--interval-minutes", type=int, default=15)
    p.add_argument("--batch-size", type=int, default=2000)
    p.add_argument("--seed", type=int, default=11)
    p.add_argument("--ca-cert", default=os.environ.get("REQUESTS_CA_BUNDLE"))
    return p.parse_args()


def cluster_stats_doc(cluster: dict, ts: datetime, store_bytes: int, shard_count: int) -> dict:
    return {
        "@timestamp": ts.isoformat(),
        "event": {"dataset": "elasticsearch.cluster.stats", "module": "elasticsearch"},
        "metricset": {"name": "cluster_stats", "period": 10000},
        "service": {"type": "elasticsearch"},
        "elasticsearch": {
            "cluster": {
                "id": cluster["id"],
                "name": cluster["name"],
                "stats": {
                    "indices": {
                        "shards": {"count": shard_count, "primaries": shard_count},
                        "store": {"size": {"bytes": store_bytes}},
                    }
                },
            }
        },
    }


def node_stats_doc(
    cluster: dict, ts: datetime, heap_pct: float, heap_max: int, queue: int, disk_total: int
) -> dict:
    heap_used = int(heap_max * heap_pct / 100.0)
    return {
        "@timestamp": ts.isoformat(),
        "event": {"dataset": "elasticsearch.node.stats", "module": "elasticsearch"},
        "metricset": {"name": "node_stats", "period": 10000},
        "service": {"type": "elasticsearch"},
        "elasticsearch": {
            "cluster": {"id": cluster["id"], "name": cluster["name"]},
            "node": {
                "name": f"es-{cluster['name'].replace('-cluster', '')}",
                "stats": {
                    "jvm": {
                        "mem": {
                            "heap": {
                                "max": {"bytes": heap_max},
                                "used": {"bytes": heap_used, "pct": int(heap_pct)},
                            },
                        }
                    },
                    "thread_pool": {"write": {"queue": {"count": queue}}},
                    "fs": {"total": {"total": {"bytes": disk_total}}},
                },
            },
        },
    }


def bulk_batch(docs: list[dict]) -> str:
    lines = []
    for doc in docs:
        lines.append(json.dumps({"create": {"_index": MONITORING_INDEX}}))
        lines.append(json.dumps(doc))
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=args.days)
    step = timedelta(minutes=args.interval_minutes)
    verify = args.ca_cert if args.ca_cert else True
    auth = (args.user, args.password)

    session = requests.Session()
    session.get(f"{args.host.rstrip('/')}/_cluster/health", auth=auth, verify=verify, timeout=30).raise_for_status()

    docs: list[dict] = []
    ts = start
    bucket = 0
    while ts <= now:
        day_frac = (ts - start).total_seconds() / max(1.0, (now - start).total_seconds())
        for cluster in CLUSTERS:
            store = int(
                cluster["store_base"]
                + cluster["store_growth_per_day"] * (ts - start).days
                + cluster["store_growth_per_day"] * day_frac
                + rng.uniform(-0.02, 0.02) * cluster["store_base"]
            )
            shards = cluster["shards_base"] + int((ts - start).days * 0.3)
            heap = cluster["heap_base"] + 12 * math.sin(bucket / 18.0 + hash(cluster["id"]) % 7)
            heap += rng.uniform(-4, 4)
            heap = max(35.0, min(88.0, heap))
            queue = max(0, int(cluster["queue_base"] + rng.expovariate(0.35) + 5 * math.sin(bucket / 9.0)))
            heap_max = 805306368 if cluster["name"] == "remote-a" else 536870912
            disk_total = 831211705753

            docs.append(cluster_stats_doc(cluster, ts, store, shards))
            docs.append(node_stats_doc(cluster, ts, heap, heap_max, queue, disk_total))
        ts += step
        bucket += 1

    print(f"Generated {len(docs):,} monitoring docs ({args.days}d @ {args.interval_minutes}m, {len(CLUSTERS)} clusters)")
    print(f"Window: {start.isoformat()} -> {now.isoformat()}")

    sent = 0
    t0 = time.perf_counter()
    while sent < len(docs):
        batch = docs[sent : sent + args.batch_size]
        r = session.post(
            f"{args.host.rstrip('/')}/_bulk",
            data=bulk_batch(batch),
            headers={"Content-Type": "application/x-ndjson"},
            auth=auth,
            verify=verify,
            timeout=180,
        )
        r.raise_for_status()
        result = r.json()
        if result.get("errors"):
            for item in result.get("items", []):
                err = (item.get("create") or {}).get("error")
                if err:
                    print(f"  bulk error: {err}", file=sys.stderr)
                    return 1
        sent += len(batch)
        print(f"  indexed {sent:>6,} / {len(docs):,}", flush=True)

    elapsed = time.perf_counter() - t0
    print(f"Done in {elapsed:.1f}s ({sent / elapsed:,.0f} docs/s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

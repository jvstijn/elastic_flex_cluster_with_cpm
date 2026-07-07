#!/usr/bin/env python3
"""
Reset CPM state on central and regenerate data streams on all remote clusters.

Naming: {type}-{dataset}-{namespace}
  type:    logs | metrics | traces
  dataset: application, billing, nginx, apache, postgres, otel, payroll, sales, redis, kafka
  namespace: {env}.{cluster_number}  e.g. prd.08  (env in tst, acc, prd, dev)

100 streams per data cluster (cluster01–cluster15); central-cluster is skipped for ingest.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_FILE = ROOT / "ansible/inventories/kaposi/files/cluster_registry.json"

TYPES = ("logs", "metrics", "traces")
DATASETS = (
    "application",
    "billing",
    "nginx",
    "apache",
    "postgres",
    "otel",
    "payroll",
    "sales",
    "redis",
    "kafka",
)
ENVS = ("tst", "acc", "prd", "dev")
STREAMS_PER_CLUSTER = 100
DOCS_PER_STREAM = 100
BULK_BATCH = 200

CPM_INDICES = [
    "cpm-pipeline-state",
    "cpm-routing-suggestions",
    "cpm-scores",
    "cpm-cluster-scores",
    "cpm-pipeline-history",
    "cpm-stream-coverage",
    "cpm-watcher-executions",
    "cpm-bytes-per-event",
]

SKIP_DATA_CLUSTERS = {"central-cluster"}

# Legacy simulator topics: logs-application08-catalog-acc (cluster# glued into dataset)
STALE_TOPIC_RE = re.compile(
    r"^(logs|metrics|traces)-[a-z]+(0[1-9]|1[0-5])-[a-z]+-(tst|acc|prd|dev)$"
)


def is_recognized_topic(topic: str) -> bool:
    """Match cpmIsRecognizedTopic() in watcher_cpm-state-manager."""
    if not topic:
        return False
    if topic == "filebeat":
        return True
    if not topic.startswith(("logs-", "metrics-", "traces-")):
        return False
    if STALE_TOPIC_RE.match(topic):
        return False
    return True


def load_env() -> tuple[str, str]:
    env_file = os.environ.get("CPM_ENV_FILE")
    if not env_file:
        for candidate in (
            ROOT / "ansible/.env",
            Path.home() / "DoD/docker/reference/.env",
        ):
            if candidate.is_file():
                env_file = str(candidate)
                break
    password = os.environ.get("ELASTIC_PASSWORD")
    if env_file and Path(env_file).is_file():
        for line in Path(env_file).read_text().splitlines():
            line = line.strip()
            if line.startswith("ELASTIC_PASSWORD="):
                password = line.split("=", 1)[1].strip().strip('"').strip("'")
                break
    if not password:
        sys.exit("Set ELASTIC_PASSWORD or CPM_ENV_FILE pointing at .env")
    central = os.environ.get("ELASTIC_BASE_URL", "https://central.kaposi.net")
    return central.rstrip("/"), password


def session_for(password: str) -> requests.Session:
    s = requests.Session()
    s.auth = ("elastic", password)
    s.headers.update({"Content-Type": "application/json"})
    return s


def es_req(
    s: requests.Session,
    method: str,
    url: str,
    *,
    data: str | None = None,
    headers: dict | None = None,
    timeout: int = 120,
) -> requests.Response:
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    r = s.request(method, url, data=data, headers=hdrs, timeout=timeout)
    if not r.ok:
        raise RuntimeError(f"{method} {url} -> {r.status_code}: {r.text[:500]}")
    return r


def cleanup_stale_pipeline_state(s: requests.Session, central: str) -> None:
    """Remove pipeline-state docs for legacy simulator topics only."""
    print("\n=== Cleanup stale pipeline-state topics ===")
    r = s.post(
        f"{central}/cpm-pipeline-state/_search?scroll=2m",
        json={"size": 500, "query": {"match_all": {}}, "_source": ["topic"]},
    )
    r.raise_for_status()
    data = r.json()
    scroll_id = data.get("_scroll_id")
    stale_ids: list[str] = []
    while True:
        for hit in data.get("hits", {}).get("hits", []):
            topic = (hit.get("_source") or {}).get("topic", "")
            if topic and not is_recognized_topic(topic):
                stale_ids.append(hit["_id"])
        if not data.get("hits", {}).get("hits"):
            break
        r = s.post(
            f"{central}/_search/scroll",
            json={"scroll": "2m", "scroll_id": scroll_id},
        )
        r.raise_for_status()
        data = r.json()
        scroll_id = data.get("_scroll_id")
    if scroll_id:
        s.delete(f"{central}/_search/scroll", json={"scroll_id": scroll_id})
    if not stale_ids:
        print("  none")
        return
    deleted = 0
    for i in range(0, len(stale_ids), 100):
        batch = stale_ids[i : i + 100]
        lines = "\n".join(
            json.dumps({"delete": {"_index": "cpm-pipeline-state", "_id": doc_id}})
            for doc_id in batch
        )
        br = s.post(
            f"{central}/_bulk?refresh=true",
            data=lines + "\n",
            headers={"Content-Type": "application/x-ndjson"},
        )
        br.raise_for_status()
        deleted += len(batch)
    print(f"  deleted {deleted} stale state documents")


def flush_cpm_indices(s: requests.Session, central: str) -> None:
    print("\n=== Flush CPM indices (central) ===")
    for index in CPM_INDICES:
        url = f"{central}/{index}/_delete_by_query?conflicts=proceed&refresh=true"
        body = json.dumps({"query": {"match_all": {}}})
        r = s.post(url, data=body)
        if r.status_code == 404:
            print(f"  skip {index} (missing)")
            continue
        r.raise_for_status()
        deleted = r.json().get("deleted", 0)
        print(f"  {index}: deleted {deleted} docs")

    # Stream locks only (keep _global and other routing config)
    url = f"{central}/cpm-routing-config/_delete_by_query?conflicts=proceed&refresh=true"
    body = json.dumps({"query": {"term": {"config_type": "stream_lock"}}})
    r = s.post(url, data=body)
    if r.status_code != 404:
        r.raise_for_status()
        print(f"  cpm-routing-config stream_locks: deleted {r.json().get('deleted', 0)}")


def delete_logstash_pipelines(s: requests.Session, central: str) -> None:
    print("\n=== Remove Logstash pipelines (central) ===")
    r = s.get(f"{central}/_logstash/pipeline")
    if r.status_code == 404:
        print("  no logstash pipeline API")
        return
    r.raise_for_status()
    pipelines = r.json().get("pipelines", [])
    if not pipelines:
        print("  none found")
        return
    for pid in pipelines:
        dr = s.delete(f"{central}/_logstash/pipeline/{pid}")
        if dr.status_code in (200, 404):
            print(f"  deleted pipeline {pid}")
        else:
            dr.raise_for_status()


def cluster_number(cluster_id: str) -> str:
    if cluster_id.startswith("cluster") and cluster_id[7:].isdigit():
        return cluster_id[7:].zfill(2)
    return cluster_id.replace("cluster", "").zfill(2) if "cluster" in cluster_id else cluster_id


def stream_names_for_cluster(cluster_id: str) -> list[str]:
    num = cluster_number(cluster_id)
    combos: list[str] = []
    for t in TYPES:
        for d in DATASETS:
            for e in ENVS:
                combos.append(f"{t}-{d}-{e}.{num}")
    return combos[:STREAMS_PER_CLUSTER]


def list_managed_data_streams(s: requests.Session, host: str) -> list[str]:
    r = s.get(f"{host}/_data_stream")
    if r.status_code == 404:
        return []
    r.raise_for_status()
    names: list[str] = []
    for ds in r.json().get("data_streams", []):
        name = ds.get("name", "")
        if name.startswith(("logs-", "metrics-", "traces-")):
            names.append(name)
    return names


def delete_data_streams(s: requests.Session, host: str, names: list[str]) -> None:
    if not names:
        return
    # ES accepts comma-separated list; chunk to avoid URL limits
    chunk = 50
    for i in range(0, len(names), chunk):
        batch = names[i : i + chunk]
        path = ",".join(batch)
        r = s.delete(f"{host}/_data_stream/{path}?expand_wildcards=all")
        if r.status_code == 404:
            continue
        r.raise_for_status()
        print(f"    deleted {len(batch)} data streams")


def gen_log_doc(rng: random.Random, seq: int, dataset: str, namespace: str, t0: datetime, t1: datetime) -> dict:
    ts = t0 + (t1 - t0) * rng.random()
    return {
        "@timestamp": ts.isoformat().replace("+00:00", "Z"),
        "message": f"seed log {dataset} #{seq}",
        "service.name": dataset,
        "service.environment": namespace,
        "log.level": rng.choice(["info", "warn", "error", "debug"]),
        "labels": {"seq": seq},
    }


def gen_metrics_doc(rng: random.Random, seq: int, dataset: str, namespace: str, t0: datetime, t1: datetime) -> dict:
    ts = t0 + (t1 - t0) * rng.random()
    return {
        "@timestamp": ts.isoformat().replace("+00:00", "Z"),
        "metricset.name": "seed",
        "service.name": dataset,
        "service.environment": namespace,
        "system.cpu.total.norm.pct": round(rng.uniform(0.05, 0.95), 4),
        "labels": {"seq": seq},
    }


def gen_trace_doc(rng: random.Random, seq: int, dataset: str, namespace: str, t0: datetime, t1: datetime) -> dict:
    ts = t0 + (t1 - t0) * rng.random()
    trace_id = f"{seq:032x}"[:32]
    span_id = f"{seq:016x}"[:16]
    return {
        "@timestamp": ts.isoformat().replace("+00:00", "Z"),
        "trace.id": trace_id,
        "span.id": span_id,
        "transaction.name": f"{dataset}-request",
        "service.name": dataset,
        "service.environment": namespace,
        "labels": {"seq": seq},
    }


GENERATORS = {
    "logs": gen_log_doc,
    "metrics": gen_metrics_doc,
    "traces": gen_trace_doc,
}


def bulk_index_stream(
    s: requests.Session,
    host: str,
    stream: str,
    count: int,
    rng: random.Random,
) -> int:
    stream_type = stream.split("-", 1)[0]
    rest = stream.split("-", 1)[1]
    dataset, namespace = rest.rsplit("-", 1)
    gen = GENERATORS[stream_type]
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(days=2)
    t1 = now
    errors = 0
    sent = 0
    while sent < count:
        n = min(BULK_BATCH, count - sent)
        lines: list[str] = []
        for i in range(n):
            doc = gen(rng, sent + i + 1, dataset, namespace, t0, t1)
            lines.append(json.dumps({"create": {"_index": stream}}))
            lines.append(json.dumps(doc))
        body = "\n".join(lines) + "\n"
        r = s.post(
            f"{host}/_bulk",
            data=body,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=180,
        )
        r.raise_for_status()
        result = r.json()
        if result.get("errors"):
            for item in result.get("items", []):
                action = item.get("create") or {}
                if "error" in action:
                    errors += 1
        sent += n
    return errors


def seed_cluster(
    s: requests.Session,
    cluster_id: str,
    host: str,
    *,
    dry_run: bool,
) -> None:
    streams = stream_names_for_cluster(cluster_id)
    print(f"\n=== {cluster_id} @ {host} ({len(streams)} streams) ===")
    if dry_run:
        print(f"  sample: {streams[0]}, {streams[1]}, ... {streams[-1]}")
        return

    existing = list_managed_data_streams(s, host)
    if existing:
        print(f"  removing {len(existing)} existing managed data streams")
        delete_data_streams(s, host, existing)

    rng = random.Random(hash(cluster_id) & 0xFFFFFFFF)
    total_errors = 0
    for idx, stream in enumerate(streams, 1):
        total_errors += bulk_index_stream(s, host, stream, DOCS_PER_STREAM, rng)
        if idx % 25 == 0 or idx == len(streams):
            print(f"  indexed {idx}/{len(streams)} streams (errors={total_errors})")
    print(f"  done: {len(streams)} streams × {DOCS_PER_STREAM} docs")


def run_bootstrap_chain(s: requests.Session, central: str) -> None:
    print("\n=== Run CPM watcher chain (manual triggers) ===")
    watchers = [
        "cpm-registry-sync",
        "cpm-scoring",
        "cpm-routing-advisor",
        "cpm-state-manager",
        "cpm-pipeline-manager",
    ]
    for wid in watchers:
        url = f"{central}/_watcher/watch/{wid}/_execute"
        r = s.put(url, data="{}")
        if r.status_code == 404:
            print(f"  skip {wid} (not found)")
            continue
        r.raise_for_status()
        print(f"  executed {wid}")
        time.sleep(2)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-flush", action="store_true")
    parser.add_argument("--skip-pipelines", action="store_true")
    parser.add_argument("--skip-seed", action="store_true")
    parser.add_argument("--cleanup-stale-state", action="store_true")
    parser.add_argument("--bootstrap", action="store_true", help="Run watcher chain after seed")
    parser.add_argument("--registry", type=Path, default=REGISTRY_FILE)
    args = parser.parse_args()

    central, password = load_env()
    registry = json.loads(args.registry.read_text())
    s = session_for(password)

    print(f"Central: {central}")
    health = s.get(f"{central}/_cluster/health", timeout=30)
    health.raise_for_status()
    print(f"Cluster health: {health.json().get('status')}")

    if not args.dry_run:
        if not args.skip_flush:
            flush_cpm_indices(s, central)
        if not args.skip_pipelines:
            delete_logstash_pipelines(s, central)
        if args.cleanup_stale_state:
            cleanup_stale_pipeline_state(s, central)

    data_clusters = [
        (cid, cfg["ingest_hosts"].rstrip("/"))
        for cid, cfg in sorted(registry.items())
        if cid not in SKIP_DATA_CLUSTERS and cfg.get("ingest_hosts")
    ]
    print(f"\nData clusters: {len(data_clusters)} (skipping {', '.join(SKIP_DATA_CLUSTERS)})")

    if not args.skip_seed:
        for cluster_id, host in data_clusters:
            seed_cluster(s, cluster_id, host, dry_run=args.dry_run)

    if args.bootstrap and not args.dry_run:
        cleanup_stale_pipeline_state(s, central)
        run_bootstrap_chain(s, central)

    if not args.dry_run:
        total_streams = len(data_clusters) * STREAMS_PER_CLUSTER
        print(f"\n=== Complete ===")
        print(f"  {total_streams} data streams across {len(data_clusters)} clusters")
        print(f"  naming: {{type}}-{{dataset}}-{{env}}.{{NN}}")
        if not args.bootstrap:
            print("  Run with --bootstrap to trigger watcher chain, or ansible-playbook bootstrap.yml")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

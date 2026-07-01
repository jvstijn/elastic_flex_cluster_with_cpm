#!/usr/bin/env python3
"""Bootstrap CPM pipeline chain - DEPRECATED: use ansible/bootstrap.yml

Seeds templates, syncs registry, patches ingest_hosts/dc, runs the full watcher
chain, and removes the bootstrap kafka-to-central pipeline.

Usage:
    python3 docker/scripts/bootstrap_cpm_pipelines.py
    python3 docker/scripts/bootstrap_cpm_pipelines.py --settings cpm/cpm_settings.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SETTINGS = ROOT / "cpm" / "cpm_settings.json"
DEFAULT_CONFIGS = ROOT / "cpm" / "cpm_configs.json"
DEFAULT_CA = ROOT / "cpm" / "docker-ca.crt"

DOCKER_REGISTRY = {
    "central-cluster": {"ingest_hosts": "https://es-central", "dc": "dc-central"},
    "remote-a": {"ingest_hosts": "https://es-remote-a", "dc": "dc-a"},
    "remote-b": {"ingest_hosts": "https://es-remote-b", "dc": "dc-b"},
}


def load_json(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


def main() -> int:
    p = argparse.ArgumentParser(description="Bootstrap CPM pipelines for Docker stack")
    p.add_argument("--settings", default=str(DEFAULT_SETTINGS))
    p.add_argument("--configs", default=str(DEFAULT_CONFIGS))
    p.add_argument("--ca", default=str(DEFAULT_CA))
    p.add_argument("--keep-bootstrap-pipeline", action="store_true",
                   help="Keep kafka-to-central bootstrap pipeline")
    args = p.parse_args()

    settings = load_json(Path(args.settings))
    bundle = load_json(Path(args.configs))
    registry_cfg = settings.get("cluster_registry") or DOCKER_REGISTRY
    es = settings["es_host"].rstrip("/")
    headers = {
        "Authorization": f"ApiKey {settings['es_api_key']}",
        "Content-Type": "application/json",
    }
    verify = args.ca if os.path.isfile(args.ca) else False
    session = requests.Session()
    session.headers.update(headers)
    session.verify = verify

    def call(method: str, path: str, body=None, ok404: bool = False):
        r = session.request(method, f"{es}{path}", json=body, timeout=60)
        if ok404 and r.status_code == 404:
            return None
        if not r.ok:
            print(f"  FAIL {method} {path}: {r.status_code} {r.text[:400]}")
            return None
        return r.json() if r.text else {}

    def patch_registry() -> None:
        search = call("POST", "/cpm-cluster-registry/_search", {"size": 20, "query": {"match_all": {}}})
        if not search:
            return
        for hit in search["hits"]["hits"]:
            src = hit["_source"]
            name = src.get("cluster_name", "")
            defaults = registry_cfg.get(name) or registry_cfg.get(src.get("cluster_id", ""))
            if not defaults:
                continue
            doc = {**src, **defaults}
            if call("PUT", f"/cpm-cluster-registry/_doc/{hit['_id']}?refresh=wait_for", doc):
                print(f"  {name}: ingest_hosts={doc.get('ingest_hosts')} dc={doc.get('dc')}")

    print("=== Ensure template mapping ===")
    call("PUT", "/cpm-pipeline-templates/_mapping", {
        "properties": {"consumer_threads": {"type": "integer"}},
    })

    print("\n=== Seed pipeline templates ===")
    for tpl_id, tpl in bundle.get("templates", {}).items():
        if call("PUT", f"/cpm-pipeline-templates/_doc/{tpl_id}", tpl):
            print(f"  template {tpl_id}")

    print("\n=== Sync cluster registry from monitoring ===")
    if call("POST", "/_watcher/watch/cpm-registry-sync/_execute", {}):
        print("  cpm-registry-sync executed")

    print("\n=== Patch cluster registry (ingest_hosts / dc) ===")
    patch_registry()

    print("\n=== Seed logs-beats-raw on central catchall ===")
    central = call("POST", "/cpm-cluster-registry/_search", {
        "size": 1,
        "query": {"term": {"cluster_name": "central-cluster"}},
    })
    central_id = None
    central_dc = "dc-central"
    if central and central["hits"]["hits"]:
        src = central["hits"]["hits"][0]["_source"]
        central_id = src["cluster_id"]
        central_dc = src.get("dc") or central_dc
        beats_state = {
            "data_stream_type": "logs",
            "dataset": "beats",
            "namespace": "raw",
            "pipeline_type": "catchall",
            "pipeline_id": f"{central_dc}_cpm-catchall-{central_id}",
            "cluster_id": central_id,
            "dc": central_dc,
            "topic": "logs-beats-raw",
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if call("PUT", "/cpm-pipeline-state/_doc/logs-beats-raw", beats_state):
            print("  logs-beats-raw → central catchall")

    watchers = [
        "cpm-scoring",
        "cpm-routing-advisor",
        "cpm-state-manager",
    ]
    print("\n=== Execute CPM watchers ===")
    for name in watchers:
        result = call("POST", f"/_watcher/watch/{name}/_execute", {})
        if result:
            state = result.get("watch_record", {}).get("status", {}).get("execution_state", "?")
            print(f"  {name}: {state}")
        time.sleep(1)

    print("\n=== Re-patch registry before pipeline push ===")
    patch_registry()

    print("\n=== Push Logstash pipelines ===")
    # Remove stale CPM pipelines (including prior bootstrap/default ids)
    existing = call("GET", "/_logstash/pipeline")
    if isinstance(existing, dict):
        for pid in list(existing):
            if "cpm-" in pid or pid == "kafka-to-central":
                session.delete(f"{es}/_logstash/pipeline/{pid}", timeout=30)
    result = call("POST", "/_watcher/watch/cpm-pipeline-manager/_execute", {})
    if result:
        state = result.get("watch_record", {}).get("status", {}).get("execution_state", "?")
        print(f"  cpm-pipeline-manager: {state}")

    if not args.keep_bootstrap_pipeline:
        print("\n=== Remove bootstrap pipeline ===")
        r = session.delete(f"{es}/_logstash/pipeline/kafka-to-central", timeout=30)
        if r.status_code in (200, 404):
            print("  removed kafka-to-central (or already absent)")

    print("\n=== Verification ===")
    pipes = call("GET", "/_logstash/pipeline")
    if isinstance(pipes, dict):
        print(f"  logstash pipelines ({len(pipes)}): {', '.join(sorted(pipes))}")

    state = call("POST", "/cpm-pipeline-state/_search", {"size": 50, "sort": [{"updated_at": "desc"}]})
    if state:
        print(f"  pipeline state entries: {state['hits']['total']['value']}")
        for hit in state["hits"]["hits"][:10]:
            s = hit["_source"]
            print(f"    {s.get('topic')} → {s.get('cluster_id')[:12]}… ({s.get('pipeline_type')})")

    return 0


if __name__ == "__main__":
    sys.exit(main())

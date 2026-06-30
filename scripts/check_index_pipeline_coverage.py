#!/usr/bin/env python3
"""Check Elasticsearch indices / data streams against the topics consumed by the
CPM-managed Logstash pipelines.

For every data stream (and filebeat index) it determines the Kafka topic it maps
to (<type>-<dataset>-<namespace>, or "filebeat") and reports which Logstash
pipeline(s) consume that topic. It then shows:

  * a per-index overview: index/data-stream -> topic -> pipeline(s) -> status
  * a coverage summary: which indices are NOT read by any pipeline
  * orphan topics: topics configured in a pipeline with no matching index

Only stdlib is used. The script prompts for a username and password.

Examples:
  ./check_index_pipeline_coverage.py --insecure
  ./check_index_pipeline_coverage.py --host https://es-central:9200 --ca-cert ca.crt
  ./check_index_pipeline_coverage.py --insecure --namespace default
"""
from __future__ import annotations

import argparse
import base64
import getpass
import json
import re
import ssl
import sys
import urllib.error
import urllib.request

TOPIC_RE = re.compile(r"topics\s*=>\s*\[([^\]]*)\]")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Check ES indices/data streams vs CPM Logstash pipeline topics.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--host", default="https://localhost:9200", help="Elasticsearch base URL")
    p.add_argument("--user", help="Username (prompted if omitted)")
    p.add_argument("--ca-cert", help="Path to a CA certificate for TLS verification")
    p.add_argument("--insecure", action="store_true", help="Skip TLS verification (self-signed certs)")
    p.add_argument("--namespace", help="Only consider data streams in this namespace (e.g. default)")
    p.add_argument("--only-uncovered", action="store_true", help="Only print indices with no pipeline")
    return p.parse_args()


def make_get(host: str, user: str, pw: str, ca_cert: str | None, insecure: bool):
    ctx = ssl.create_default_context()
    if ca_cert:
        ctx.load_verify_locations(ca_cert)
    elif insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    auth = base64.b64encode(f"{user}:{pw}".encode()).decode()
    base = host.rstrip("/")

    def get(path: str):
        req = urllib.request.Request(
            base + path,
            headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, context=ctx) as resp:
            return json.loads(resp.read().decode())

    return get


def topic_for(name: str) -> str | None:
    """Kafka topic that a data stream / index name maps to, or None if not routed by CPM."""
    if name.startswith(("logs-", "metrics-", "traces-")):
        return name  # data stream name == <type>-<dataset>-<namespace> == topic
    if name.startswith("filebeat"):
        return "filebeat"
    return None


def extract_topics(pipeline_config: str) -> set[str]:
    topics: set[str] = set()
    for m in TOPIC_RE.finditer(pipeline_config or ""):
        for raw in m.group(1).split(","):
            t = raw.strip().strip('"').strip("'").strip()
            if t and not t.startswith("__"):  # skip unrendered __TOPICS_LIST__ token
                topics.add(t)
    return topics


def main() -> int:
    args = parse_args()
    user = args.user or input("Elasticsearch username: ").strip()
    pw = getpass.getpass("Elasticsearch password: ")
    get = make_get(args.host, user, pw, args.ca_cert, args.insecure)

    try:
        get("/")
    except urllib.error.HTTPError as e:
        return _fail(f"Authentication/connection failed: HTTP {e.code} {e.read().decode()[:200]}")
    except urllib.error.URLError as e:
        return _fail(f"Connection failed: {e}. For self-signed TLS use --insecure or --ca-cert.")

    # --- 1. Logstash pipelines -> topics -------------------------------------
    try:
        pipelines = get("/_logstash/pipeline") or {}
    except urllib.error.HTTPError as e:
        return _fail(f"Could not read /_logstash/pipeline: HTTP {e.code}")
    pipe_topics: dict[str, set[str]] = {}
    topic_to_pipes: dict[str, set[str]] = {}
    for pid, body in pipelines.items():
        topics = extract_topics(body.get("pipeline", ""))
        pipe_topics[pid] = topics
        for t in topics:
            topic_to_pipes.setdefault(t, set()).add(pid)

    # --- 2. Data streams + filebeat indices ----------------------------------
    names: set[str] = set()
    for d in (get("/_data_stream") or {}).get("data_streams", []):
        names.add(d["name"])
    try:  # classic filebeat indices that are not data stream backing indices
        for c in get("/_cat/indices/filebeat-*?format=json&h=index"):
            idx = c["index"]
            if not idx.startswith(".ds-"):
                names.add(idx)
    except urllib.error.HTTPError:
        pass

    routable = sorted(n for n in names if topic_for(n) is not None)
    if args.namespace:
        ns = args.namespace
        routable = [n for n in routable if n.endswith("-" + ns) or topic_for(n) == "filebeat"]

    # --- 3. Build coverage ----------------------------------------------------
    rows = []  # (index, topic, status, pipelines)
    used_topics: set[str] = set()
    covered = 0
    for n in routable:
        t = topic_for(n)
        pipes = sorted(topic_to_pipes.get(t, []))
        used_topics.add(t)
        status = "OK" if pipes else "MISSING"
        if pipes:
            covered += 1
        rows.append((n, t, status, pipes))

    orphan_topics = sorted(set(topic_to_pipes) - used_topics)

    # --- 4. Print overview ----------------------------------------------------
    print(f"\nHost: {args.host}")
    print(f"Logstash pipelines: {len(pipelines)}   |   routable indices/data streams: {len(routable)}")

    print("\n=== Logstash pipelines (topics per pipeline) ===")
    for pid in sorted(pipe_topics):
        ts = sorted(pipe_topics[pid])
        print(f"  {pid}  ({len(ts)} topics)")
        for t in ts:
            has = "" if any(topic_for(n) == t for n in routable) else "   [geen index]"
            print(f"      - {t}{has}")
    if not pipe_topics:
        print("  (geen Logstash pipelines gevonden)")

    print("\n=== Index/data-stream  ->  topic  ->  pipeline ===")
    shown = [r for r in rows if (not args.only_uncovered or r[2] == "MISSING")]
    w_idx = max([len(r[0]) for r in shown] + [16])
    w_top = max([len(r[1]) for r in shown] + [12])
    print(f"  {'INDEX / DATA STREAM'.ljust(w_idx)}  {'TOPIC'.ljust(w_top)}  {'STATUS':7}  PIPELINE(S)")
    print(f"  {'-'*w_idx}  {'-'*w_top}  {'-'*7}  {'-'*11}")
    for idx, topic, status, pipes in shown:
        print(f"  {idx.ljust(w_idx)}  {topic.ljust(w_top)}  {status:7}  {', '.join(pipes) or '-'}")

    print("\n=== Samenvatting ===")
    missing = [r[0] for r in rows if r[2] == "MISSING"]
    print(f"  routable indices/data streams : {len(routable)}")
    print(f"  gedekt door een pipeline       : {covered}")
    print(f"  ONTBREEKT (geen pipeline)      : {len(missing)}")
    if missing:
        print("  Indices zonder pipeline (hun topic wordt door geen pipeline gelezen):")
        for n in missing:
            print(f"    - {n}   (topic: {topic_for(n)})")
    if orphan_topics:
        print(f"\n  Orphan-topics (in pipeline, geen bijbehorende index): {len(orphan_topics)}")
        for t in orphan_topics:
            print(f"    - {t}   (in: {', '.join(sorted(topic_to_pipes[t]))})")

    print()
    return 1 if missing else 0


def _fail(msg: str) -> int:
    print(msg, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())

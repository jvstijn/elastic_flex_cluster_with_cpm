#!/usr/bin/env python3
"""Check Elasticsearch indices / data streams against the topics consumed by the
CPM-managed Logstash pipelines.

For every data stream (and filebeat index) that received data in the last N
hours (default 24) it determines the Kafka topic it maps to
(<type>-<dataset>-<namespace>, or "filebeat") and reports which Logstash
pipeline(s) consume that topic. It then shows:

  * a per-index overview: index/data-stream -> topic -> pipeline(s) -> status
  * a coverage summary: which indices are NOT read by any pipeline
  * orphan topics: topics configured in a pipeline with no matching index

The report is printed to the screen AND written to a file.
Only stdlib is used. The script prompts for a username and password.

Examples:
  ./check_index_pipeline_coverage.py --insecure
  ./check_index_pipeline_coverage.py --host https://es-central:9200 --ca-cert ca.crt
  ./check_index_pipeline_coverage.py --insecure --hours 24 --output coverage.txt
"""
from __future__ import annotations

import argparse
import base64
import datetime
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
    p.add_argument("--hours", type=int, default=24,
                   help="Only consider data streams that received documents in the last N hours")
    p.add_argument("--namespace", help="Only consider data streams in this namespace (e.g. default)")
    p.add_argument("--only-uncovered", action="store_true", help="Only print indices with no pipeline")
    p.add_argument("--output", help="File to write the report to "
                                    "(default: index_pipeline_coverage_<timestamp>.txt)")
    return p.parse_args()


def make_call(host: str, user: str, pw: str, ca_cert: str | None, insecure: bool):
    ctx = ssl.create_default_context()
    if ca_cert:
        ctx.load_verify_locations(ca_cert)
    elif insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    auth = base64.b64encode(f"{user}:{pw}".encode()).decode()
    base = host.rstrip("/")

    def call(method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            base + path, data=data, method=method,
            headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, context=ctx) as resp:
            return json.loads(resp.read().decode())

    return call


def topic_for(name: str) -> str | None:
    """Kafka topic that a data stream / index name maps to, or None if not routed by CPM."""
    if name.startswith(("logs-", "metrics-", "traces-")):
        return name  # data stream name == <type>-<dataset>-<namespace> == topic
    if name.startswith("filebeat"):
        return "filebeat"
    return None


def backing_to_name(idx: str) -> str:
    """`.ds-<type>-<dataset>-<namespace>-<date>-<gen>` -> `<type>-<dataset>-<namespace>`;
    `filebeat-<ver>-<date>-<gen>` -> `filebeat-<ver>`."""
    name = idx[4:] if idx.startswith(".ds-") else idx
    parts = name.split("-")
    if len(parts) >= 3:  # strip the trailing -<date>-<generation>
        name = "-".join(parts[:-2])
    return name


def active_datasets(call, hours: int) -> dict[str, int]:
    """Return {data_stream_name: doc_count} for streams with docs in the last <hours> hours."""
    body = {
        "size": 0,
        "query": {"range": {"@timestamp": {"gte": f"now-{hours}h"}}},
        "aggs": {"idx": {"terms": {"field": "_index", "size": 10000}}},
    }
    path = "/logs-*,metrics-*,traces-*,filebeat-*/_search?allow_no_indices=true&ignore_unavailable=true"
    try:
        res = call("POST", path, body)
    except urllib.error.HTTPError:
        return {}
    out: dict[str, int] = {}
    for b in res.get("aggregations", {}).get("idx", {}).get("buckets", []):
        name = backing_to_name(b["key"])
        if topic_for(name):
            out[name] = out.get(name, 0) + b["doc_count"]
    return out


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
    call = make_call(args.host, user, pw, args.ca_cert, args.insecure)

    try:
        call("GET", "/")
    except urllib.error.HTTPError as e:
        return _fail(f"Authentication/connection failed: HTTP {e.code} {e.read().decode()[:200]}")
    except urllib.error.URLError as e:
        return _fail(f"Connection failed: {e}. For self-signed TLS use --insecure or --ca-cert.")

    # --- 1. Logstash pipelines -> topics -------------------------------------
    try:
        pipelines = call("GET", "/_logstash/pipeline") or {}
    except urllib.error.HTTPError as e:
        return _fail(f"Could not read /_logstash/pipeline: HTTP {e.code}")
    pipe_topics: dict[str, set[str]] = {}
    topic_to_pipes: dict[str, set[str]] = {}
    for pid, body in pipelines.items():
        topics = extract_topics(body.get("pipeline", ""))
        pipe_topics[pid] = topics
        for t in topics:
            topic_to_pipes.setdefault(t, set()).add(pid)

    # --- 2. Data streams active in the last N hours --------------------------
    counts = active_datasets(call, args.hours)
    routable = sorted(counts)
    if args.namespace:
        ns = args.namespace
        routable = [n for n in routable if n.endswith("-" + ns) or topic_for(n) == "filebeat"]

    # --- 3. Build coverage ----------------------------------------------------
    rows = []  # (index, topic, docs, status, pipelines)
    used_topics: set[str] = set()
    covered = 0
    for n in routable:
        t = topic_for(n)
        pipes = sorted(topic_to_pipes.get(t, []))
        used_topics.add(t)
        status = "OK" if pipes else "MISSING"
        if pipes:
            covered += 1
        rows.append((n, t, counts[n], status, pipes))

    orphan_topics = sorted(set(topic_to_pipes) - used_topics)

    # --- 4. Build the report (screen + file) ---------------------------------
    report: list[str] = []

    def emit(line: str = "") -> None:
        report.append(line)

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    emit(f"CPM index <-> Logstash pipeline coverage   ({now})")
    emit(f"Host: {args.host}   |   window: last {args.hours}h")
    emit(f"Logstash pipelines: {len(pipelines)}   |   active routable data streams: {len(routable)}")

    emit("\n=== Logstash pipelines (topics per pipeline) ===")
    for pid in sorted(pipe_topics):
        ts = sorted(pipe_topics[pid])
        emit(f"  {pid}  ({len(ts)} topics)")
        for t in ts:
            tag = "" if any(topic_for(n) == t for n in routable) else "   [no active index]"
            emit(f"      - {t}{tag}")
    if not pipe_topics:
        emit("  (no Logstash pipelines found)")

    emit("\n=== Index/data-stream  ->  topic  ->  pipeline ===")
    shown = [r for r in rows if (not args.only_uncovered or r[3] == "MISSING")]
    w_idx = max([len(r[0]) for r in shown] + [16])
    w_top = max([len(r[1]) for r in shown] + [12])
    emit(f"  {'INDEX / DATA STREAM'.ljust(w_idx)}  {'TOPIC'.ljust(w_top)}  {'24h DOCS':>9}  {'STATUS':7}  PIPELINE(S)")
    emit(f"  {'-'*w_idx}  {'-'*w_top}  {'-'*9}  {'-'*7}  {'-'*11}")
    for idx, topic, docs, status, pipes in shown:
        emit(f"  {idx.ljust(w_idx)}  {topic.ljust(w_top)}  {docs:>9}  {status:7}  {', '.join(pipes) or '-'}")

    emit("\n=== Summary ===")
    missing = [r[0] for r in rows if r[3] == "MISSING"]
    emit(f"  active routable data streams (last {args.hours}h) : {len(routable)}")
    emit(f"  covered by a pipeline                       : {covered}")
    emit(f"  NOT covered (no pipeline)                   : {len(missing)}")
    if missing:
        emit("  Indices without a pipeline (their topic is read by no pipeline):")
        for n in missing:
            emit(f"    - {n}   (topic: {topic_for(n)}, {counts[n]} docs)")
    if orphan_topics:
        emit(f"\n  Orphan topics (in a pipeline, no active index): {len(orphan_topics)}")
        for t in orphan_topics:
            emit(f"    - {t}   (in: {', '.join(sorted(topic_to_pipes[t]))})")

    text = "\n".join(report)
    print(text)

    outfile = args.output or f"index_pipeline_coverage_{datetime.datetime.now():%Y%m%d_%H%M%S}.txt"
    try:
        with open(outfile, "w") as f:
            f.write(text + "\n")
        print(f"\nReport written to: {outfile}")
    except OSError as e:
        print(f"\nCould not write report file {outfile}: {e}", file=sys.stderr)

    return 1 if missing else 0


def _fail(msg: str) -> int:
    print(msg, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())

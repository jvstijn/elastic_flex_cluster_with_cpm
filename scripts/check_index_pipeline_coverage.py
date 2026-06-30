#!/usr/bin/env python3
"""Check Elasticsearch indices / data streams against the topics consumed by the
CPM-managed Logstash pipelines.

Index names are taken from the stack-monitoring data (default
monitoring:.monitoring-es-8-mb*) so indices on remote clusters are included too,
and only indices that run on an ACTIVE cluster (active=true in
cpm-cluster-registry) are considered.

For every data stream / filebeat index seen in monitoring in the last N hours
(default 24) it determines:

  * the Kafka topic it maps to (<type>-<dataset>-<namespace>, or "filebeat"),
  * on which cluster it runs (elasticsearch.cluster.name + cluster_uuid),
  * which Logstash pipeline(s) consume that topic.

It then shows a per-index overview, a coverage summary (indices read by no
pipeline) and orphan topics (pipeline topics with no matching index).

The report is printed to the screen AND written to a file.
Only stdlib is used. The script prompts for a username and password.

Examples:
  ./check_index_pipeline_coverage.py --insecure
  ./check_index_pipeline_coverage.py --host https://es-central:9200 --ca-cert ca.crt
  ./check_index_pipeline_coverage.py --insecure --hours 24 --output coverage.txt
  ./check_index_pipeline_coverage.py --insecure --registry-index cpmw-cluster-registry
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
    p.add_argument("--monitoring-index", default="monitoring:.monitoring-es-8-mb*",
                   help="Index pattern with the stack-monitoring data to read index names from")
    p.add_argument("--registry-index", default="cpm-cluster-registry",
                   help="Cluster registry index; only clusters with active=true are considered")
    p.add_argument("--hours", type=int, default=24,
                   help="Only consider indices seen in monitoring in the last N hours")
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


def _top(bucket_agg) -> str | None:
    b = (bucket_agg or {}).get("buckets", [])
    return b[0]["key"] if b else None


def active_clusters(call, registry_index: str):
    """Return (active_uuids:set, active_names:set, found:bool) from the cluster registry."""
    body = {"size": 1000, "query": {"term": {"active": True}},
            "_source": ["cluster_id", "cluster_uuid", "cluster_name"]}
    try:
        res = call("POST", f"/{registry_index}/_search", body)
    except urllib.error.HTTPError:
        return set(), set(), False
    uuids: set[str] = set()
    names: set[str] = set()
    for h in res.get("hits", {}).get("hits", []):
        s = h.get("_source", {})
        for k in ("cluster_uuid", "cluster_id"):
            if s.get(k):
                uuids.add(s[k])
        if s.get("cluster_name"):
            names.add(s["cluster_name"])
    return uuids, names, True


def monitored_datasets(call, hours: int, monitoring_index: str) -> dict[str, dict]:
    """Read index names + owning cluster from stack monitoring.

    Returns {data_stream_name: {(cluster_name, uuid): docs}}.
    """
    body = {
        "size": 0,
        "query": {"bool": {"filter": [
            {"match_phrase": {"event.dataset": "elasticsearch.index"}},
            {"range": {"@timestamp": {"gte": f"now-{hours}h"}}},
            {"bool": {"should": [
                {"wildcard": {"elasticsearch.index.name": ".ds-logs-*"}},
                {"wildcard": {"elasticsearch.index.name": ".ds-metrics-*"}},
                {"wildcard": {"elasticsearch.index.name": ".ds-traces-*"}},
                {"wildcard": {"elasticsearch.index.name": ".ds-filebeat-*"}},
                {"wildcard": {"elasticsearch.index.name": "filebeat-*"}},
            ], "minimum_should_match": 1}},
        ]}},
        "aggs": {"idx": {
            "terms": {"field": "elasticsearch.index.name", "size": 10000},
            "aggs": {
                "cname": {"terms": {"field": "elasticsearch.cluster.name", "size": 10}},
                "cuuid": {"terms": {"field": "cluster_uuid", "size": 10}},
                "cid":   {"terms": {"field": "elasticsearch.cluster.id", "size": 10}},
                "docs":  {"max": {"field": "elasticsearch.index.total.docs.count"}},
            },
        }},
    }
    path = "/" + monitoring_index + "/_search?allow_no_indices=true&ignore_unavailable=true"
    res = call("POST", path, body)
    streams: dict[str, dict] = {}
    for b in res.get("aggregations", {}).get("idx", {}).get("buckets", []):
        name = backing_to_name(b["key"])
        if not topic_for(name):
            continue
        cname = _top(b.get("cname")) or "?"
        uuid = _top(b.get("cuuid")) or _top(b.get("cid")) or "?"
        docs = int((b.get("docs") or {}).get("value") or 0)
        d = streams.setdefault(name, {})
        d[(cname, uuid)] = d.get((cname, uuid), 0) + docs
    return streams


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

    # --- 2. Active clusters from the registry --------------------------------
    auuids, anames, reg_found = active_clusters(call, args.registry_index)
    reg_note = ""
    if not reg_found:
        reg_note = f"registry {args.registry_index} not found - NOT filtering on active clusters"
    elif not (auuids or anames):
        reg_note = f"registry {args.registry_index} has no active clusters - result will be empty"

    # --- 3. Index names + cluster from monitoring (last N hours) --------------
    try:
        streams = monitored_datasets(call, args.hours, args.monitoring_index)
    except urllib.error.HTTPError as e:
        return _fail(f"Could not query monitoring index {args.monitoring_index!r}: "
                     f"HTTP {e.code} {e.read().decode()[:200]}")

    # keep only indices on an active cluster (when the registry is available)
    if reg_found:
        filtered: dict[str, dict] = {}
        for name, clmap in streams.items():
            keep = {k: v for k, v in clmap.items()
                    if k[1] in auuids or k[0] in anames}
            if keep:
                filtered[name] = keep
        streams = filtered

    routable = sorted(streams)
    if args.namespace:
        ns = args.namespace
        routable = [n for n in routable if n.endswith("-" + ns) or topic_for(n) == "filebeat"]

    # --- 4. Build coverage ----------------------------------------------------
    rows = []  # (name, topic, cluster_str, uuid_str, docs, status, pipelines)
    used_topics: set[str] = set()
    covered = 0
    for n in routable:
        t = topic_for(n)
        pipes = sorted(topic_to_pipes.get(t, []))
        used_topics.add(t)
        clusters = sorted(streams[n])
        cluster_str = ",".join(cn for cn, _ in clusters) or "?"
        uuid_str = ",".join(uu for _, uu in clusters) or "?"
        docs = sum(streams[n].values())
        status = "OK" if pipes else "MISSING"
        if pipes:
            covered += 1
        rows.append((n, t, cluster_str, uuid_str, docs, status, pipes))

    orphan_topics = sorted(set(topic_to_pipes) - used_topics)

    # --- 5. Build the report (screen + file) ---------------------------------
    report: list[str] = []

    def emit(line: str = "") -> None:
        report.append(line)

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    emit(f"CPM index <-> Logstash pipeline coverage   ({now})")
    emit(f"Host: {args.host}   |   monitoring: {args.monitoring_index}   |   window: last {args.hours}h")
    emit(f"Active-cluster registry: {args.registry_index}   "
         f"(active clusters: {len(anames) if reg_found else '-'})")
    if reg_note:
        emit(f"NOTE: {reg_note}")
    emit(f"Logstash pipelines: {len(pipelines)}   |   indices on active clusters: {len(routable)}")

    emit("\n=== Logstash pipelines (topics per pipeline) ===")
    for pid in sorted(pipe_topics):
        ts = sorted(pipe_topics[pid])
        emit(f"  {pid}  ({len(ts)} topics)")
        for t in ts:
            tag = "" if any(topic_for(n) == t for n in routable) else "   [no active index]"
            emit(f"      - {t}{tag}")
    if not pipe_topics:
        emit("  (no Logstash pipelines found)")

    emit("\n=== Index/data-stream  ->  cluster  ->  topic  ->  pipeline ===")
    shown = [r for r in rows if (not args.only_uncovered or r[5] == "MISSING")]
    w_idx = max([len(r[0]) for r in shown] + [16])
    w_top = max([len(r[1]) for r in shown] + [12])
    w_cl = max([len(r[2]) for r in shown] + [7])
    w_uu = max([len(r[3]) for r in shown] + [12])
    emit(f"  {'INDEX / DATA STREAM'.ljust(w_idx)}  {'CLUSTER'.ljust(w_cl)}  "
         f"{'CLUSTER_UUID'.ljust(w_uu)}  {'TOPIC'.ljust(w_top)}  {'DOCS':>10}  {'STATUS':7}  PIPELINE(S)")
    emit(f"  {'-'*w_idx}  {'-'*w_cl}  {'-'*w_uu}  {'-'*w_top}  {'-'*10}  {'-'*7}  {'-'*11}")
    for name, topic, cluster_str, uuid_str, docs, status, pipes in shown:
        emit(f"  {name.ljust(w_idx)}  {cluster_str.ljust(w_cl)}  {uuid_str.ljust(w_uu)}  "
             f"{topic.ljust(w_top)}  {docs:>10}  {status:7}  {', '.join(pipes) or '-'}")

    emit("\n=== Summary ===")
    missing = [r[0] for r in rows if r[5] == "MISSING"]
    emit(f"  indices on active clusters (last {args.hours}h) : {len(routable)}")
    emit(f"  covered by a pipeline                     : {covered}")
    emit(f"  NOT covered (no pipeline)                 : {len(missing)}")
    if missing:
        emit("  Indices without a pipeline (their topic is read by no pipeline):")
        for n in missing:
            cl = ",".join(cn for cn, _ in sorted(streams[n]))
            emit(f"    - {n}   (topic: {topic_for(n)}, cluster: {cl})")
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

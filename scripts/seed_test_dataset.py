#!/usr/bin/env python3
"""Fill the "test-dataset" Kafka topic with the generated test data.

Reads the data streams seen in stack monitoring over the last N hours (the data
that gen_monitoring_testdata.py produced), and produces one event per dataset to
the "test-dataset" topic. Each event carries a data_stream object
(type/dataset/namespace) derived from the data-stream name, so the router
(logstash-router) can route it to <type>-<dataset>-<namespace> (or to
dead-letter-queue when that topic does not exist).

Only stdlib is used; prompts for the Elasticsearch password (or use --password).

Examples:
  ./seed_test_dataset.py --insecure
  ./seed_test_dataset.py --insecure --hours 24 --dry-run
"""
from __future__ import annotations

import argparse
import base64
import datetime
import getpass
import json
import random
import ssl
import subprocess
import sys
import urllib.error
import urllib.request


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--host", default="https://localhost:9200")
    p.add_argument("--user", default="elastic")
    p.add_argument("--password")
    p.add_argument("--ca-cert")
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--monitoring-index", default="monitoring:.monitoring-es-8-mb*")
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--container", default="dod-elastic-kafka-1")
    p.add_argument("--topic", default="test-dataset")
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--producer", default="/opt/kafka/bin/kafka-console-producer.sh")
    p.add_argument("--dry-run", action="store_true", help="Print events, do not produce")
    return p.parse_args()


def backing_to_name(idx: str) -> str:
    name = idx[4:] if idx.startswith(".ds-") else idx
    parts = name.split("-")
    if len(parts) >= 3:
        name = "-".join(parts[:-2])
    return name


def parse_data_stream(name: str):
    """`logs-system.auth-default` -> (logs, system.auth, default);
    `logs-vmware` -> (logs, vmware, default)."""
    fd = name.find("-")
    if fd <= 0:
        return "logs", name, "default"
    typ = name[:fd]
    rest = name[fd + 1:]
    ld = rest.rfind("-")
    if ld <= 0:
        return typ, rest, "default"
    return typ, rest[:ld], rest[ld + 1:]


def monitored_streams(call, hours, monitoring_index):
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
                "cname": {"terms": {"field": "elasticsearch.cluster.name", "size": 1}},
                "docs": {"max": {"field": "elasticsearch.index.total.docs.count"}},
            },
        }},
    }
    path = "/" + monitoring_index + "/_search?allow_no_indices=true&ignore_unavailable=true"
    res = call("POST", path, body)
    out = {}
    for b in res.get("aggregations", {}).get("idx", {}).get("buckets", []):
        name = backing_to_name(b["key"])
        cn = b.get("cname", {}).get("buckets", [])
        cluster = cn[0]["key"] if cn else "?"
        docs = int((b.get("docs") or {}).get("value") or 0)
        # keep the largest docs / first cluster per stream name
        if name not in out:
            out[name] = {"cluster": cluster, "docs": docs}
    return out


def make_call(host, user, pw, ca_cert, insecure):
    ctx = ssl.create_default_context()
    if ca_cert:
        ctx.load_verify_locations(ca_cert)
    elif insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    auth = base64.b64encode(f"{user}:{pw}".encode()).decode()

    def call(method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(host.rstrip("/") + path, data=data, method=method,
                                     headers={"Authorization": "Basic " + auth,
                                              "Content-Type": "application/json"})
        with urllib.request.urlopen(req, context=ctx) as resp:
            return json.loads(resp.read().decode())

    return call


def main() -> int:
    args = parse_args()
    pw = args.password or getpass.getpass("Elasticsearch password: ")
    call = make_call(args.host, args.user, pw, args.ca_cert, args.insecure)
    rnd = random.Random(20260701)

    try:
        streams = monitored_streams(call, args.hours, args.monitoring_index)
    except urllib.error.URLError as e:
        print(f"Could not query monitoring: {e}", file=sys.stderr)
        return 2

    now = datetime.datetime.now(datetime.timezone.utc)
    lines = []
    for name in sorted(streams):
        info = streams[name]
        typ, ds, ns = parse_data_stream(name)
        ts = now - datetime.timedelta(seconds=rnd.randint(0, args.hours * 3600))
        evt = {
            "@timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "message": f"generated test event for {name}",
            "data_stream": {"type": typ, "dataset": ds, "namespace": ns},
            "origin": {"cluster": info["cluster"]},
            "metrics": {"docs_count": info["docs"]},
        }
        lines.append(json.dumps(evt))

    print(f"data streams in monitoring (last {args.hours}h): {len(streams)}")
    print(f"events to produce to '{args.topic}': {len(lines)}")
    if args.dry_run:
        for l in lines[:5]:
            print("  " + l)
        print("  ... (dry-run, nothing produced)")
        return 0

    cmd = ["docker", "exec", "-i", args.container, args.producer,
           "--bootstrap-server", args.bootstrap, "--topic", args.topic]
    try:
        r = subprocess.run(cmd, input=("\n".join(lines) + "\n").encode(), capture_output=True)
    except FileNotFoundError:
        print("docker CLI not found on PATH.", file=sys.stderr)
        return 2
    if r.returncode != 0:
        sys.stderr.write(r.stderr.decode())
        return 1
    print(f"produced {len(lines)} events to '{args.topic}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

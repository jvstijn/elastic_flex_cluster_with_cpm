#!/usr/bin/env python3
"""Create the CPM Kafka topics.

Reads the topics consumed by the CPM-managed Logstash pipelines from
Elasticsearch (`GET /_logstash/pipeline`) and creates each one in the Kafka
cluster via `kafka-topics.sh` inside a running broker container.

Idempotent (`--create --if-not-exists`). Only stdlib is used; the script
prompts for the Elasticsearch password (or use --password).

Examples:
  ./create_kafka_topics.py --insecure
  ./create_kafka_topics.py --insecure --partitions 3 --replication-factor 3
  ./create_kafka_topics.py --insecure --dry-run
"""
from __future__ import annotations

import argparse
import base64
import getpass
import json
import re
import ssl
import subprocess
import sys
import urllib.error
import urllib.request

TOPIC_RE = re.compile(r"topics\s*=>\s*\[([^\]]*)\]")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--host", default="https://localhost:9200", help="Elasticsearch base URL")
    p.add_argument("--user", default="elastic", help="Elasticsearch username")
    p.add_argument("--password", help="Elasticsearch password (prompted if omitted)")
    p.add_argument("--ca-cert", help="CA certificate for TLS verification")
    p.add_argument("--insecure", action="store_true", help="Skip TLS verification")
    p.add_argument("--container", default="dod-elastic-kafka-1",
                   help="Kafka broker container to run kafka-topics.sh in")
    p.add_argument("--bootstrap", default="kafka:9092", help="Kafka bootstrap server (inside the container)")
    p.add_argument("--partitions", type=int, default=1)
    p.add_argument("--replication-factor", type=int, default=3)
    p.add_argument("--kafka-topics", default="/opt/kafka/bin/kafka-topics.sh",
                   help="Path to kafka-topics.sh inside the container")
    p.add_argument("--dry-run", action="store_true", help="Only list the topics, do not create them")
    return p.parse_args()


def extract_topics(pipeline_config: str) -> set[str]:
    topics: set[str] = set()
    for m in TOPIC_RE.finditer(pipeline_config or ""):
        for raw in m.group(1).split(","):
            t = raw.strip().strip('"').strip("'").strip()
            if t and not t.startswith("__"):
                topics.add(t)
    return topics


def get_topics(host: str, user: str, pw: str, ca_cert: str | None, insecure: bool) -> list[str]:
    ctx = ssl.create_default_context()
    if ca_cert:
        ctx.load_verify_locations(ca_cert)
    elif insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    auth = base64.b64encode(f"{user}:{pw}".encode()).decode()
    req = urllib.request.Request(host.rstrip("/") + "/_logstash/pipeline",
                                 headers={"Authorization": "Basic " + auth})
    with urllib.request.urlopen(req, context=ctx) as resp:
        pipelines = json.loads(resp.read().decode()) or {}
    topics: set[str] = set()
    for _pid, body in pipelines.items():
        topics |= extract_topics(body.get("pipeline", ""))
    return sorted(topics)


def main() -> int:
    args = parse_args()
    pw = args.password or getpass.getpass("Elasticsearch password: ")
    try:
        topics = get_topics(args.host, args.user, pw, args.ca_cert, args.insecure)
    except urllib.error.URLError as e:
        print(f"Could not read /_logstash/pipeline: {e}", file=sys.stderr)
        return 2

    print(f"CPM pipeline topics found: {len(topics)}")
    for t in topics:
        print(f"  {t}")
    if not topics:
        print("No topics to create.")
        return 0
    if args.dry_run:
        print("\n(dry-run) not creating anything.")
        return 0

    # Create all topics in a single exec (loop reads topic names from stdin).
    script = (
        'ok=0; err=0; '
        'while IFS= read -r t; do [ -z "$t" ] && continue; '
        f'if {args.kafka_topics} --bootstrap-server {args.bootstrap} --create --if-not-exists '
        f'--topic "$t" --partitions {args.partitions} --replication-factor {args.replication_factor} '
        '>/dev/null 2>&1; then ok=$((ok+1)); else err=$((err+1)); echo "FAILED: $t" >&2; fi; '
        'done; echo "created/exists: $ok  failed: $err"'
    )
    cmd = ["docker", "exec", "-i", args.container, "bash", "-c", script]
    print(f"\nCreating {len(topics)} topics in container {args.container} ...")
    try:
        r = subprocess.run(cmd, input="\n".join(topics).encode(), capture_output=True)
    except FileNotFoundError:
        print("docker CLI not found on PATH.", file=sys.stderr)
        return 2
    sys.stdout.write(r.stdout.decode())
    if r.stderr:
        sys.stderr.write(r.stderr.decode())
    if r.returncode != 0:
        return 1

    # Show the resulting topic count in the cluster.
    count_cmd = ["docker", "exec", args.container, "bash", "-c",
                 f"{args.kafka_topics} --bootstrap-server {args.bootstrap} --list | wc -l"]
    c = subprocess.run(count_cmd, capture_output=True)
    if c.returncode == 0:
        print(f"topics now in the cluster: {c.stdout.decode().strip()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

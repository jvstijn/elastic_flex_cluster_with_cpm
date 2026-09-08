#!/usr/bin/env python3
"""Generate data-stream events and produce them straight into their Kafka topic.

For every dataset x namespace combination this writes JSON events carrying a
`data_stream` object to the topic `<type>-<dataset>-<namespace><suffix>`. The
CPM-managed Logstash pipelines consume those topics with `data_stream => true`,
so the events land in the matching data stream on whichever cluster CPM assigned
the topic to.

Unlike fill_kafka_events.py (which produces random bytes with
kafka-producer-perf-test.sh, good for volume but not parseable as JSON) the
events here are real documents. And unlike seed_test_dataset.py it does not read
existing streams from monitoring, nor does it go through the router on the
test-dataset topic — it produces to the destination topic directly, which is
what makes it work for the acceptance topics too (`--suffix=-acc`, since the
router does not know about the extension).

Only stdlib is used.

Examples:
  ./gen_stream_data.py --dry-run
  ./gen_stream_data.py --ensure-topics --count 500
  ./gen_stream_data.py --ensure-topics --suffix=-acc --count 500
  ./gen_stream_data.py --namespaces default,ns1 --datasets logs:nginx,metrics:system
"""
from __future__ import annotations

import argparse
import datetime
import json
import random
import subprocess
import sys

# type:dataset — de zes datasets die in de omgeving rondgingen, nu los van de
# namespace zodat elke combinatie gemaakt kan worden.
DEFAULT_DATASETS = [
    "logs:nginx",
    "logs:nginx.access",
    "logs:webapp",
    "logs:firewall",
    "logs:application.auditd",
    "metrics:system",
]

DEFAULT_NAMESPACES = ["default", "ns1", "ns2", "ns3"]

LOG_LEVELS = ["info", "info", "info", "warn", "error", "debug"]
HTTP_PATHS = ["/", "/api/v1/status", "/login", "/assets/app.js", "/health", "/orders"]
HTTP_CODES = [200, 200, 200, 201, 301, 404, 500]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--container", default="dod-elastic-kafka-1",
                   help="Kafka broker container to produce through")
    p.add_argument("--bootstrap", default="kafka:9092,kafka2:9092,kafka3:9092")
    p.add_argument("--producer", default="/opt/kafka/bin/kafka-console-producer.sh")
    p.add_argument("--topics-tool", default="/opt/kafka/bin/kafka-topics.sh")
    p.add_argument("--datasets", default=",".join(DEFAULT_DATASETS),
                   help="comma-separated type:dataset pairs")
    p.add_argument("--namespaces", default=",".join(DEFAULT_NAMESPACES),
                   help="comma-separated namespaces")
    p.add_argument("--suffix", default="",
                   help='topic name extension, e.g. "-acc" for the acceptance environment')
    p.add_argument("--only-topics", default="",
                   help="comma-separated topic names WITHOUT the suffix; limits the run to "
                        "these. Combine with --count to give one group of streams a different "
                        "volume than another (an uneven load for the routing-advisor to level "
                        "out again)")
    p.add_argument("--count", type=int, default=250, help="events per dataset+namespace")
    p.add_argument("--hours", type=int, default=6,
                   help="spread the timestamps over the last N hours")
    p.add_argument("--partitions", type=int, default=1)
    p.add_argument("--replication-factor", type=int, default=3)
    p.add_argument("--ensure-topics", action="store_true",
                   help="create the topics first (--create --if-not-exists)")
    p.add_argument("--seed", type=int, default=20260827)
    p.add_argument("--dry-run", action="store_true", help="print a sample, produce nothing")
    return p.parse_args()


def combos(datasets: str, namespaces: str, suffix: str, only: str = ""):
    """[(topic, type, dataset, namespace), ...] for every combination."""
    wanted = {t.strip() for t in only.split(",") if t.strip()}
    out = []
    for pair in (d.strip() for d in datasets.split(",") if d.strip()):
        typ, _, ds = pair.partition(":")
        if not ds:
            typ, ds = "logs", typ
        for ns in (n.strip() for n in namespaces.split(",") if n.strip()):
            base = f"{typ}-{ds}-{ns}"
            if wanted and base not in wanted:
                continue
            out.append((base + suffix, typ, ds, ns))
    if wanted:
        missing = wanted - {t[: len(t) - len(suffix)] if suffix else t for t, _, _, _ in out}
        if missing:
            print(f"warning: --only-topics not matched: {sorted(missing)}", file=sys.stderr)
    return out


def make_event(rnd, now, hours, typ, dataset, namespace):
    ts = now - datetime.timedelta(seconds=rnd.randint(0, max(1, hours * 3600)))
    host = f"{dataset.split('.')[0]}-{namespace}-{rnd.randint(1, 3):02d}"
    evt = {
        "@timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "data_stream": {"type": typ, "dataset": dataset, "namespace": namespace},
        "event": {"dataset": dataset, "module": dataset.split(".")[0]},
        "host": {"name": host},
        "service": {"name": dataset.split(".")[0], "environment": namespace},
    }
    if typ == "metrics":
        evt["system"] = {
            "cpu": {"total": {"pct": round(rnd.uniform(0.05, 0.95), 3)}},
            "memory": {"actual": {"used": {"pct": round(rnd.uniform(0.2, 0.9), 3)}}},
            "load": {"1": round(rnd.uniform(0.1, 4.0), 2)},
        }
        evt["message"] = f"system metrics sample from {host}"
    elif "nginx" in dataset:
        code = rnd.choice(HTTP_CODES)
        path = rnd.choice(HTTP_PATHS)
        evt["http"] = {"response": {"status_code": code, "bytes": rnd.randint(120, 48000)}}
        evt["url"] = {"path": path}
        evt["source"] = {"ip": f"10.{rnd.randint(0,4)}.{rnd.randint(0,255)}.{rnd.randint(1,254)}"}
        evt["log"] = {"level": "error" if code >= 500 else "info"}
        evt["message"] = f'GET {path} HTTP/1.1" {code}'
    else:
        level = rnd.choice(LOG_LEVELS)
        evt["log"] = {"level": level}
        evt["message"] = f"[{level}] {dataset} event on {host}"
    return evt


def ensure_topics(args, topics) -> int:
    script = (
        'ok=0; err=0; '
        'while IFS= read -r t; do [ -z "$t" ] && continue; '
        f'if {args.topics_tool} --bootstrap-server {args.bootstrap.split(",")[0]} '
        f'--create --if-not-exists --topic "$t" --partitions {args.partitions} '
        f'--replication-factor {args.replication_factor} >/dev/null 2>&1; '
        'then ok=$((ok+1)); else err=$((err+1)); echo "FAILED: $t" >&2; fi; '
        'done; echo "topics created/exists: $ok  failed: $err"'
    )
    r = subprocess.run(["docker", "exec", "-i", args.container, "bash", "-c", script],
                       input=("\n".join(topics) + "\n").encode())
    return r.returncode


def main() -> int:
    args = parse_args()
    rnd = random.Random(args.seed)
    now = datetime.datetime.now(datetime.timezone.utc)
    targets = combos(args.datasets, args.namespaces, args.suffix, args.only_topics)
    if not targets:
        print("Nothing to generate.", file=sys.stderr)
        return 2

    total = len(targets) * args.count
    label = f'"{args.suffix}"' if args.suffix else "(no suffix)"
    print(f"topics: {len(targets)}   events each: {args.count}   total: {total}   suffix: {label}")
    for topic, _, _, _ in targets:
        print(f"  {topic}")

    if args.dry_run:
        topic, typ, ds, ns = targets[0]
        print(f"\nsample event for {topic}:")
        print("  " + json.dumps(make_event(rnd, now, args.hours, typ, ds, ns)))
        print("(dry-run, nothing produced)")
        return 0

    if args.ensure_topics:
        print("\nensuring topics exist ...")
        if ensure_topics(args, [t for t, _, _, _ in targets]) != 0:
            print("Could not create the topics.", file=sys.stderr)
            return 1

    # One producer call per topic; the console producer takes one JSON per line.
    failures = 0
    for topic, typ, ds, ns in targets:
        lines = "\n".join(
            json.dumps(make_event(rnd, now, args.hours, typ, ds, ns))
            for _ in range(args.count)
        )
        cmd = ["docker", "exec", "-i", args.container, args.producer,
               "--bootstrap-server", args.bootstrap, "--topic", topic]
        try:
            r = subprocess.run(cmd, input=(lines + "\n").encode(), capture_output=True)
        except FileNotFoundError:
            print("docker CLI not found on PATH.", file=sys.stderr)
            return 2
        if r.returncode != 0:
            failures += 1
            sys.stderr.write(f"FAILED {topic}: {r.stderr.decode().strip()[:200]}\n")
        else:
            print(f"  produced {args.count} -> {topic}")

    print(f"\ndone: {len(targets) - failures}/{len(targets)} topics filled")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Fill the Kafka cluster with events.

Produces `--total` events randomly distributed over the existing data topics,
and additionally `--big-count` events to each of `--big-topics` (4 by default).
Events are produced directly into the topics with kafka-producer-perf-test.sh
running inside a broker container (parallelised). test-dataset and
dead-letter-queue are excluded so the router is not triggered.

Only stdlib is used.

Examples:
  ./fill_kafka_events.py                 # 1,000,000 spread + 4x 200,000
  ./fill_kafka_events.py --dry-run
  ./fill_kafka_events.py --total 1000000 --big-count 200000 \
      --big-topics logs-system.auth-default,logs-nginx-prod,...
"""
from __future__ import annotations

import argparse
import collections
import random
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--container", default="dod-elastic-kafka-1")
    p.add_argument("--bootstrap", default="kafka:9092,kafka2:9092,kafka3:9092")
    p.add_argument("--topics-tool", default="/opt/kafka/bin/kafka-topics.sh")
    p.add_argument("--perf-tool", default="/opt/kafka/bin/kafka-producer-perf-test.sh")
    p.add_argument("--total", type=int, default=1_000_000, help="events spread randomly over topics")
    p.add_argument("--big-count", type=int, default=200_000, help="extra events per big topic")
    p.add_argument("--big-topics", help="comma-separated topics for the extra events (default: auto-pick 4)")
    p.add_argument("--record-size", type=int, default=200)
    p.add_argument("--parallel", type=int, default=4, help="concurrent perf-test producers")
    p.add_argument("--exclude", default="test-dataset,dead-letter-queue",
                   help="comma-separated topics to skip")
    p.add_argument("--seed", type=int, default=20260701)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def list_topics(container, tool, bootstrap):
    out = subprocess.check_output(
        ["docker", "exec", container, tool, "--bootstrap-server", bootstrap.split(",")[0], "--list"]
    ).decode()
    return [t.strip() for t in out.splitlines() if t.strip() and not t.startswith("__")]


def main() -> int:
    args = parse_args()
    excl = {t.strip() for t in args.exclude.split(",") if t.strip()}
    try:
        topics = [t for t in list_topics(args.container, args.topics_tool, args.bootstrap) if t not in excl]
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"Could not list topics: {e}", file=sys.stderr)
        return 2
    if not topics:
        print("No topics to fill.", file=sys.stderr)
        return 2

    # pick the big topics
    if args.big_topics:
        big = [t for t in (x.strip() for x in args.big_topics.split(",")) if t]
        missing = [t for t in big if t not in topics]
        if missing:
            print(f"big-topics not found: {missing}", file=sys.stderr)
            return 2
    else:
        prefer = ["logs-system.auth-default", "logs-winlog.winlog-default",
                  "logs-nginx-prod", "metrics-oracle.performance-default"]
        big = [t for t in prefer if t in topics]
        for t in topics:  # top up to 4 if any preferred are missing
            if len(big) >= 4:
                break
            if t not in big:
                big.append(t)
        big = big[:4]

    # random distribution of --total over all topics
    rnd = random.Random(args.seed)
    counts = collections.Counter(rnd.choices(topics, k=args.total))
    for t in big:
        counts[t] += args.big_count

    plan = [(t, counts.get(t, 0)) for t in topics]
    grand_total = sum(c for _, c in plan)
    print(f"topics: {len(topics)}   spread total: {args.total}   "
          f"big topics ({len(big)}) +{args.big_count} each   grand total: {grand_total}")
    print("big topics:")
    for t in big:
        print(f"  {t}: {counts[t]}")
    if args.dry_run:
        top = sorted(plan, key=lambda x: -x[1])[:6]
        print("sample per-topic counts (top 6):")
        for t, c in top:
            print(f"  {t}: {c}")
        print("(dry-run, nothing produced)")
        return 0

    # produce: printf the plan, xargs -P runs perf-test per topic inside the broker
    lines = "".join(f"{t} {c}\n" for t, c in plan if c > 0)
    script = (
        'export KAFKA_HEAP_OPTS="-Xmx256m"; '
        f'printf %s "$PLAN" | xargs -P {args.parallel} -n 2 bash -c \''
        f'{args.perf_tool} --topic "$0" --num-records "$1" --record-size {args.record_size} '
        f'--throughput -1 --producer-props bootstrap.servers={args.bootstrap} acks=1 '
        '>/dev/null 2>&1 && echo "ok $0 $1" || echo "FAIL $0 $1"\''
    )
    full = f'PLAN={_shquote(lines)}\n{script}\n'
    print(f"producing {grand_total} events with {args.parallel} parallel producers ...")
    r = subprocess.run(["docker", "exec", "-i", args.container, "bash"], input=full.encode())
    return r.returncode


def _shquote(s: str) -> str:
    return "'" + s.replace("'", "'\\''") + "'"


if __name__ == "__main__":
    sys.exit(main())

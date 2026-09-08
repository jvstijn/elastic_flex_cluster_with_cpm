#!/usr/bin/env python3
"""Keep producing events so the routing-advisor sees a non-zero ingest rate.

The advisor derives its event rate from the GROWTH of
`elasticsearch.index.total.indexing.index_total` between two 1-hour buckets in
stack monitoring, and skips every stream whose rate is 0:

    if (tb.size() >= 2) { ... if (diff > 0.0) delta = (long)diff; }
    if (rate <= 0L) continue;

A single bulk load therefore produces nothing to advise on: the counter jumps
once and then sits still. This script produces a smaller batch every interval,
so the counter keeps climbing and each stream gets a measurable rate.

The per-group rates are deliberately different, so the advisor can rank streams
and pick the busiest ones — that ranking is what turns a stream into a dedicated
pipeline.

Only stdlib is used; it shells out to gen_stream_data.py.

Examples:
  ./simulate_continuous_load.py --rounds 6 --interval 30
  ./simulate_continuous_load.py --suffix=-acc --rounds 6 --interval 30
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

# Dezelfde 16/5/3 groepsindeling als de scheve verdeling, met oplopende volumes
# zodat de advisor onderscheid kan maken tussen drukke en rustige streams.
NS = ["default", "ns1", "ns2", "ns3"]
GROUPS = [
    ("central", [f"logs-{d}-{n}" for d in ["nginx", "nginx.access", "webapp", "firewall"] for n in NS], 120),
    ("remote-a", [f"logs-application.auditd-{n}" for n in NS] + ["metrics-system-default"], 60),
    ("remote-b", [f"metrics-system-{n}" for n in ["ns1", "ns2", "ns3"]], 25),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--suffix", default="", help='topic extension, e.g. "-acc"')
    p.add_argument("--rounds", type=int, default=6, help="number of batches")
    p.add_argument("--interval", type=int, default=30, help="seconds between batches")
    p.add_argument("--scale", type=float, default=1.0, help="multiply every batch size")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    gen = str(Path(__file__).with_name("gen_stream_data.py"))
    total = 0
    for group, streams, per in GROUPS:
        total += int(per * args.scale) * len(streams)
    print(f"{args.rounds} rondes, elke {args.interval}s, {total:,} events per ronde "
          f"({total * args.rounds:,} totaal){' [dry-run]' if args.dry_run else ''}")

    for r in range(1, args.rounds + 1):
        for group, streams, per in GROUPS:
            count = max(1, int(per * args.scale))
            cmd = [sys.executable, gen, "--only-topics", ",".join(streams),
                   "--count", str(count), "--hours", "1", "--seed", str(20260827 + r)]
            if args.suffix:
                cmd.append(f"--suffix={args.suffix}")
            if args.dry_run:
                cmd.append("--dry-run")
            r_ = subprocess.run(cmd, capture_output=True)
            ok = r_.returncode == 0
            print(f"  ronde {r}/{args.rounds} {group:9} {len(streams):2} streams x {count:4}"
                  f"  {'ok' if ok else 'FOUT'}")
            if not ok:
                sys.stderr.write(r_.stderr.decode()[:300] + "\n")
        if args.dry_run:
            break
        if r < args.rounds:
            time.sleep(args.interval)

    print("klaar — laat metricbeat de groei oppikken (10s interval) en draai daarna "
          "cpm_run_now.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())

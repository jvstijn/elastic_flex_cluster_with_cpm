#!/usr/bin/env python3
"""Copy Stack Monitoring history from one cluster to another.

When monitoring moves to a dedicated cluster, the new cluster starts empty. The
CPM weighting depends on that history: the ML jobs bucket it per 15 minutes and
the routing-advisor compares two hourly buckets. Without history every cluster
scores 0 and nothing gets weighed until enough hours have passed.

This scrolls `.monitoring-es-8-*` on the source and bulk-writes into the data
stream of the same name on the destination. Writes use op_type "create", which
is what a data stream requires, and keep the original _id so a re-run overwrites
nothing and adds no duplicates.

Only stdlib is used.

Examples:
  ./copy_monitoring_history.py --dry-run
  ./copy_monitoring_history.py --since now-6h
  ./copy_monitoring_history.py --src https://localhost:9200 --dst https://localhost:9203
"""
from __future__ import annotations

import argparse
import base64
import json
import ssl
import sys
import time
import urllib.error
import urllib.request

DATA_STREAM = ".monitoring-es-8-mb"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--src", default="https://localhost:9200", help="cluster to read from")
    p.add_argument("--dst", default="https://localhost:9203", help="cluster to write to")
    p.add_argument("--user", default="elastic")
    p.add_argument("--password", default="", help="defaults to $ELASTIC_PASSWORD")
    p.add_argument("--index", default=".monitoring-es-8-*", help="source index pattern")
    p.add_argument("--data-stream", default=DATA_STREAM, help="destination data stream")
    p.add_argument("--since", default="now-6h",
                   help="only copy documents newer than this (ES date math)")
    p.add_argument("--batch", type=int, default=2000, help="documents per scroll page")
    p.add_argument("--insecure", action="store_true", default=True,
                   help="skip TLS verification (self-signed lab certs)")
    p.add_argument("--dry-run", action="store_true", help="only count, write nothing")
    return p.parse_args()


def make_opener(insecure: bool):
    ctx = ssl.create_default_context()
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))


def call(opener, base, path, user, pw, body=None, method="GET", raw=False):
    url = base.rstrip("/") + path
    data = None
    if body is not None:
        data = (body if raw else json.dumps(body)).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/x-ndjson" if raw else "application/json")
    token = base64.b64encode(f"{user}:{pw}".encode()).decode()
    req.add_header("Authorization", "Basic " + token)
    try:
        with opener.open(req, timeout=120) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return {"error": e.read().decode()[:400], "status": e.code}


def main() -> int:
    args = parse_args()
    pw = args.password or __import__("os").environ.get("ELASTIC_PASSWORD", "")
    if not pw:
        print("Geen wachtwoord: gebruik --password of zet ELASTIC_PASSWORD.", file=sys.stderr)
        return 2
    op = make_opener(args.insecure)

    q = {"query": {"range": {"@timestamp": {"gte": args.since}}}}
    cnt = call(op, args.src, f"/{args.index}/_count", args.user, pw, q, "POST")
    if "count" not in cnt:
        print("Bron niet leesbaar:", cnt, file=sys.stderr)
        return 1
    total = cnt["count"]
    print(f"bron {args.src}  {args.index}  sinds {args.since}: {total:,} docs")

    before = call(op, args.dst, f"/{args.index}/_count", args.user, pw)
    print(f"doel {args.dst}  nu: {before.get('count', 0):,} docs")
    if args.dry_run:
        print("(dry-run, niets geschreven)")
        return 0
    if total == 0:
        return 0

    body = dict(q, size=args.batch, sort=["_doc"])
    page = call(op, args.src, f"/{args.index}/_search?scroll=5m", args.user, pw, body, "POST")
    if "error" in page:
        print("Scroll mislukt:", page, file=sys.stderr)
        return 1

    sent = failed = 0
    t0 = time.time()
    while True:
        hits = page.get("hits", {}).get("hits", [])
        if not hits:
            break
        lines = []
        for h in hits:
            lines.append(json.dumps({"create": {"_index": args.data_stream, "_id": h["_id"]}}))
            lines.append(json.dumps(h["_source"]))
        res = call(op, args.dst, "/_bulk?refresh=false", args.user, pw,
                   "\n".join(lines) + "\n", "POST", raw=True)
        if "items" not in res:
            print("Bulk mislukt:", str(res)[:300], file=sys.stderr)
            return 1
        for it in res["items"]:
            st = it["create"].get("status", 0)
            # 409 = document bestaat al; dat is precies wat we willen bij een herstart
            if st in (200, 201, 409):
                sent += 1
            else:
                failed += 1
        rate = sent / max(1e-9, time.time() - t0)
        print(f"\r  {sent:,}/{total:,} gekopieerd  ({rate:,.0f}/s)  fouten: {failed}",
              end="", flush=True)
        sid = page.get("_scroll_id")
        page = call(op, args.src, "/_search/scroll", args.user, pw,
                    {"scroll": "5m", "scroll_id": sid}, "POST")
        if "error" in page:
            break
    print()

    after = call(op, args.dst, f"/{args.index}/_count", args.user, pw)
    print(f"doel nu: {after.get('count', 0):,} docs   fouten: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

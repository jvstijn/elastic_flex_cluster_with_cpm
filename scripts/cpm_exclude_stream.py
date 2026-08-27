#!/usr/bin/env python3
"""Exclude data streams from CPM management.

Writes documents to the `cpm-stream-exclusions` index. The CPM state-manager reads
that index and drops every matching topic, so an excluded stream disappears from
`cpm-pipeline-state` and from the `topics => [...]` list in the generated Logstash
pipelines.

The pattern is the topic name, which equals the data stream name
(`<type>-<dataset>-<namespace>`, e.g. `logs-nginx-prod`). A single `*` is allowed
and matches as prefix, suffix or both:

    logs-winlog.winlog-default    exact
    logs-winlog.*                 prefix
    *-tst                         suffix
    logs-*-prod                   prefix + suffix

Changes only take effect after the state-manager and pipeline-manager run:

    ./cpm_run_now.py --insecure --only state-manager,pipeline-manager

Removing an exclusion does not automatically bring the stream back: it returns only
if it is still visible in Stack Monitoring's discovery window. See
docs/runbook-stream-routing.md section C.

Only stdlib is used; the script prompts for the Elasticsearch password (or use
--password).

Examples:
  ./cpm_exclude_stream.py --insecure list
  ./cpm_exclude_stream.py --insecure add logs-winlog.winlog-default --reason "ticket Y"
  ./cpm_exclude_stream.py --insecure add '*-tst' --reason "alle test-streams"
  ./cpm_exclude_stream.py --insecure remove logs-winlog.winlog-default
  ./cpm_exclude_stream.py --insecure check logs-nginx-prod
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
from datetime import datetime, timezone

INDEX = "cpm-stream-exclusions"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default="https://localhost:9200", help="Elasticsearch base URL")
    p.add_argument("--user", default="elastic", help="Elasticsearch username")
    p.add_argument("--password", help="Elasticsearch password (prompted if omitted)")
    p.add_argument("--ca-cert", help="CA certificate for TLS verification")
    p.add_argument("--insecure", action="store_true", help="Skip TLS verification")
    p.add_argument("--updated-by", default="cpm_exclude_stream.py",
                   help="Value for the updated_by field")

    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("add", help="Add or update an exclusion")
    sp.add_argument("pattern", help="Topic/data stream name, may contain one *")
    sp.add_argument("--reason", default="", help="Why this stream is excluded")

    sp = sub.add_parser("remove", help="Remove an exclusion")
    sp.add_argument("pattern", help="The pattern to remove")

    sub.add_parser("list", help="List all exclusions")

    sp = sub.add_parser("check", help="Show which exclusion patterns match a topic")
    sp.add_argument("topic", help="Topic/data stream name to test")

    return p.parse_args()


def make_client(args: argparse.Namespace):
    ctx = ssl.create_default_context()
    if args.ca_cert:
        ctx.load_verify_locations(args.ca_cert)
    elif args.insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    pw = args.password or getpass.getpass("Elasticsearch password: ")
    auth = base64.b64encode(f"{args.user}:{pw}".encode()).decode()
    base = args.host.rstrip("/")

    def call(path: str, method: str = "GET", body=None, ok=(200, 201)):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(base + path, data=data, method=method,
                                     headers={"Authorization": "Basic " + auth,
                                              "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, context=ctx) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else {})
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    return call


def doc_id(pattern: str) -> str:
    """Document id derived from the pattern; '*' is not valid in a URL path segment."""
    return pattern.replace("*", "_STAR_")


def matches(topic: str, pattern: str) -> bool:
    """Same matching rules as the state-manager watcher (Painless)."""
    if "*" not in pattern:
        return topic == pattern
    star = pattern.index("*")
    pre, suf = pattern[:star], pattern[star + 1:]
    if not pre:
        return topic.endswith(suf)
    if not suf:
        return topic.startswith(pre)
    return topic.startswith(pre) and topic.endswith(suf)


def fetch_all(call) -> list[dict]:
    status, body = call(f"/{INDEX}/_search?size=1000", ok=(200, 404))
    if status == 404:
        return []
    return [h["_source"] | {"_id": h["_id"]} for h in body.get("hits", {}).get("hits", [])]


def main() -> int:
    args = parse_args()
    call = make_client(args)

    if args.command == "add":
        if args.pattern.count("*") > 1:
            print("At most one '*' is supported by the state-manager matcher.", file=sys.stderr)
            return 2
        body = {
            "topic_pattern": args.pattern,
            "reason": args.reason,
            "updated_by": args.updated_by,
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        status, resp = call(f"/{INDEX}/_doc/{doc_id(args.pattern)}?refresh=true",
                            "PUT", body, ok=(200, 201))
        if status not in (200, 201):
            print(f"Failed ({status}): {json.dumps(resp)[:300]}", file=sys.stderr)
            return 1
        print(f"{resp.get('result', 'ok')}: {args.pattern}")

    elif args.command == "remove":
        status, resp = call(f"/{INDEX}/_doc/{doc_id(args.pattern)}?refresh=true",
                            "DELETE", ok=(200, 404))
        if status == 404 or resp.get("result") == "not_found":
            print(f"not found: {args.pattern}", file=sys.stderr)
            return 1
        print(f"deleted: {args.pattern}")

    elif args.command == "list":
        rows = fetch_all(call)
        if not rows:
            print("No exclusions.")
            return 0
        width = max(len(r.get("topic_pattern", "")) for r in rows)
        print(f"{'PATTERN'.ljust(width)}  {'UPDATED':20}  REASON")
        for r in sorted(rows, key=lambda x: x.get("topic_pattern", "")):
            print(f"{r.get('topic_pattern', '').ljust(width)}  "
                  f"{r.get('updated_at', ''):20}  {r.get('reason', '')}")
        print(f"\n{len(rows)} exclusion(s).")

    elif args.command == "check":
        rows = fetch_all(call)
        hits = [r["topic_pattern"] for r in rows
                if r.get("topic_pattern") and matches(args.topic, r["topic_pattern"])]
        if hits:
            print(f"{args.topic} is EXCLUDED by: {', '.join(hits)}")
        else:
            print(f"{args.topic} is not excluded ({len(rows)} pattern(s) checked).")

    if args.command in ("add", "remove"):
        print("\nRun the CPM cycle to apply:")
        print("  ./cpm_run_now.py --insecure --only state-manager,pipeline-manager")
    return 0


if __name__ == "__main__":
    sys.exit(main())

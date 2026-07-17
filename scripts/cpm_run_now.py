#!/usr/bin/env python3
"""Run the CPM cycle on demand (any time of day).

The CPM watchers/workflows run once per day on a schedule. This triggers the
same cycle immediately and in dependency order, so newly seen datasets/topics
get picked up without waiting for the daily run.

By default it force-executes the watchers (cpm-*). Use --workflows to run the
native workflows (cpmw-*) via the Kibana Workflows API instead.

Order: register-sync -> forecast-trigger -> scoring -> routing-advisor ->
state-manager -> pipeline-manager. Each CPM index is refreshed between steps so
the next stage sees fresh data. Failures are reported but do not abort the chain.

Examples:
  ./cpm_run_now.py --insecure
  ./cpm_run_now.py --insecure --only state-manager,pipeline-manager
  ./cpm_run_now.py --insecure --workflows --kibana http://localhost:5601
"""
from __future__ import annotations

import argparse
import base64
import getpass
import json
import ssl
import sys
import time
import urllib.error
import urllib.request

CHAIN = [
    "register-sync",
    "forecast-trigger",
    "scoring",
    "routing-advisor",
    "state-manager",
    "pipeline-manager",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--host", default="https://localhost:9200", help="Elasticsearch base URL")
    p.add_argument("--user", default="elastic")
    p.add_argument("--password", help="Elasticsearch password (prompted if omitted)")
    p.add_argument("--ca-cert")
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--only", help="Comma-separated subset of the chain "
                                  "(e.g. state-manager,pipeline-manager)")
    p.add_argument("--workflows", action="store_true",
                   help="Run the cpmw-* workflows via Kibana instead of the cpm-* watchers")
    p.add_argument("--kibana", default="http://localhost:5601", help="Kibana base URL (for --workflows)")
    return p.parse_args()


def make_ctx(ca_cert, insecure):
    ctx = ssl.create_default_context()
    if ca_cert:
        ctx.load_verify_locations(ca_cert)
    elif insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def caller(base, user, pw, ctx):
    auth = base64.b64encode(f"{user}:{pw}".encode()).decode()

    def call(method, path, body=None, extra_headers=None):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Authorization": "Basic " + auth, "Content-Type": "application/json"}
        if extra_headers:
            headers.update(extra_headers)
        req = urllib.request.Request(base.rstrip("/") + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, context=ctx) as resp:
                return resp.status, json.loads(resp.read().decode() or "{}")
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read().decode())
            except Exception:
                return e.code, {}

    return call


def run_watchers(call, steps):
    body = {"record_execution": True, "action_modes": {"_all": "force_execute"}}
    ok = True
    for name in steps:
        wid = "cpm-" + name
        status, res = call("POST", f"/_watcher/watch/{wid}/_execute", body)
        wr = res.get("watch_record", {})
        state = wr.get("state", f"http {status}")
        acts = wr.get("result", {}).get("actions", [])
        succeeded = [a for a in acts if a.get("status") == "success"]
        failed = [a for a in acts if a.get("status") == "failure"]
        flag = "OK " if state == "executed" and not failed else "!! "
        detail = ", ".join(f"{a['id']}={a['status']}" for a in acts) or state
        print(f"  {flag}{wid}: {detail}")
        if failed:
            ok = False
        # refresh CPM indices so the next stage sees fresh data
        call("POST", "/cpm-*/_refresh")
    return ok


def run_workflows(call, kb_call, steps):
    # map workflow name -> id
    status, res = kb_call("GET", "/api/workflows?size=1000", extra_headers={"kbn-xsrf": "true"})
    results = res.get("results", []) if isinstance(res, dict) else []
    by_name = {}
    for w in results:
        nm = w.get("name", "")
        by_name.setdefault(nm.rsplit("-", 1)[0] if nm[-1:].isdigit() else nm, w["id"])
    ok = True
    for name in steps:
        wfname = "cpmw-" + name
        wid = by_name.get(wfname)
        if not wid:
            print(f"  ?? {wfname}: workflow not found")
            ok = False
            continue
        s, r = kb_call("POST", f"/api/workflows/workflow/{wid}/run",
                       body={"inputs": {}}, extra_headers={"kbn-xsrf": "true"})
        exid = r.get("workflowExecutionId")
        print(f"  {'OK ' if exid else '!! '}{wfname}: "
              f"{'started ' + exid if exid else 'http ' + str(s)}")
        if not exid:
            ok = False
        else:
            call("POST", "/cpmw-*/_refresh")
            time.sleep(2)  # small gap so a dependent stage sees the writes
    return ok


def main() -> int:
    args = parse_args()
    steps = [s.strip() for s in args.only.split(",")] if args.only else CHAIN
    bad = [s for s in steps if s not in CHAIN]
    if bad:
        print(f"Unknown step(s): {bad}. Valid: {', '.join(CHAIN)}", file=sys.stderr)
        return 2
    pw = args.password or getpass.getpass("Elasticsearch password: ")
    ctx = make_ctx(args.ca_cert, args.insecure)
    call = caller(args.host, args.user, pw, ctx)

    print(f"Running CPM {'workflows (cpmw-*)' if args.workflows else 'watchers (cpm-*)'} "
          f"now, in order: {', '.join(steps)}")
    if args.workflows:
        kb_call = caller(args.kibana, args.user, pw, ctx)
        ok = run_workflows(call, kb_call, steps)
    else:
        ok = run_watchers(call, steps)
    print("done." if ok else "done (with warnings — see above).")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

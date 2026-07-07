#!/usr/bin/env python3
"""Bulk-index dummy Linux auditd logs into an Elasticsearch data stream."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

DATA_STREAM = "logs-application.auditd-tst"
DATASET = "application.auditd"
NAMESPACE = "tst"

HOSTNAMES = ("web-01", "web-02", "api-01", "db-01", "batch-01", "jump-01")
USERS = ("root", "admin", "deploy", "appuser", "auditd", "sshd", "nginx")
COMM_EXES = (
    ("sshd", "/usr/sbin/sshd"),
    ("sudo", "/usr/bin/sudo"),
    ("bash", "/usr/bin/bash"),
    ("python3", "/usr/bin/python3"),
    ("systemd", "/usr/lib/systemd/systemd"),
    ("curl", "/usr/bin/curl"),
    ("nginx", "/usr/sbin/nginx"),
    ("crond", "/usr/sbin/crond"),
)
SYSCALLS = (2, 59, 257, 41, 42, 0, 1, 3, 4, 5, 9, 10, 12, 21, 22, 44, 63, 82, 87)
PATHS = (
    "/etc/passwd",
    "/etc/shadow",
    "/etc/ssh/sshd_config",
    "/var/log/audit/audit.log",
    "/usr/bin/sudo",
    "/usr/bin/bash",
    "/home/appuser/.ssh/authorized_keys",
    "/tmp/.X11-unix/X0",
    "/var/www/html/index.html",
    "/etc/nginx/nginx.conf",
)
COMMANDS = (
    ("ls", "-la", "/var/log"),
    ("cat", "/etc/passwd"),
    ("sudo", "-u", "appuser", "/usr/bin/systemctl", "status", "nginx"),
    ("curl", "-s", "http://localhost/health"),
    ("python3", "-m", "pip", "list"),
    ("grep", "Failed", "/var/log/auth.log"),
    ("useradd", "-m", "svc_account"),
    ("chmod", "600", "/home/appuser/.ssh/id_rsa"),
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simulate auditd log indexing load")
    p.add_argument("--host", default=os.environ.get("ES_HOST", "https://localhost:9200"))
    p.add_argument("--user", default=os.environ.get("ES_USER", "elastic"))
    p.add_argument("--password", default=os.environ.get("ES_PASSWORD", "changeme-elastic"))
    p.add_argument("--api-key", default=os.environ.get("ES_API_KEY"))
    p.add_argument("--count", type=int, default=1_000_000)
    p.add_argument("--batch-size", type=int, default=5_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--ca-cert", default=os.environ.get("REQUESTS_CA_BUNDLE"))
    p.add_argument("--days-back", type=float, default=1.0, help="Spread timestamps over this window")
    return p.parse_args()


def auth_headers(args: argparse.Namespace) -> dict[str, str]:
    if args.api_key:
        return {"Authorization": f"ApiKey {args.api_key}"}
    return {}


def auth_tuple(args: argparse.Namespace) -> tuple[str, str] | None:
    if args.api_key:
        return None
    return (args.user, args.password)


def audit_ts(base: datetime, offset_sec: float) -> str:
    ts = base + timedelta(seconds=offset_sec)
    epoch = ts.timestamp()
    return f"{epoch:.3f}"


def hex_proctitle(argv: tuple[str, ...]) -> str:
    return "".join(f"{part.encode().hex()}00" for part in argv)


def gen_audit_line(rng: random.Random, seq: int, base: datetime) -> tuple[str, dict]:
    """Return one auditd log line and supporting ECS-style fields."""
    hostname = rng.choice(HOSTNAMES)
    uid = rng.choice((0, 1000, 1001, 1002, 33, 999))
    user = USERS[0] if uid == 0 else rng.choice(USERS[1:])
    auid = rng.choice((4294967295, 1000, 1001, 1002))
    pid = rng.randint(1000, 65000)
    ppid = rng.randint(1, 4000)
    ses = rng.randint(1, 5000)
    audit_id = 1_000_000 + seq
    offset = rng.uniform(0, 86400 * max(0.01, 1.0))
    ts_str = audit_ts(base, offset)
    comm, exe = rng.choice(COMM_EXES)
    kind = rng.randrange(8)

    if kind == 0:
        sc = rng.choice(SYSCALLS)
        success = rng.choice(("yes", "no"))
        exit_code = 0 if success == "yes" else rng.choice((-1, 1, 2, 13))
        line = (
            f"type=SYSCALL msg=audit({ts_str}:{audit_id}): arch=c000003e syscall={sc} "
            f"success={success} exit={exit_code} a0={rng.randrange(16**8):x} "
            f"items={rng.randint(0, 4)} ppid={ppid} pid={pid} auid={auid} uid={uid} "
            f"gid={rng.randint(0, 1000)} euid={uid} suid={uid} fsuid={uid} "
            f"egid={rng.randint(0, 1000)} sgid={rng.randint(0, 1000)} "
            f"fsgid={rng.randint(0, 1000)} tty=(none) ses={ses} comm=\"{comm}\" "
            f"exe=\"{exe}\" subj=unconfined_u:unconfined_r:unconfined_t:s0-s0:c0.c1023 key=\"{rng.choice(('session', 'sudo', 'access', 'identity'))}\""
        )
        event_action = "syscall"
    elif kind == 1:
        argv = rng.choice(COMMANDS)
        args_fmt = " ".join(f"a{i}=\"{arg}\"" for i, arg in enumerate(argv))
        line = f"type=EXECVE msg=audit({ts_str}:{audit_id}): argc={len(argv)} {args_fmt}"
        comm = argv[0]
        event_action = "execve"
    elif kind == 2:
        cwd = rng.choice(("/root", "/home/appuser", "/var/log", "/tmp", "/etc/nginx"))
        line = f"type=CWD msg=audit({ts_str}:{audit_id}): cwd=\"{cwd}\""
        event_action = "cwd"
    elif kind == 3:
        path = rng.choice(PATHS)
        inode = rng.randint(1000, 9_999_999)
        mode = rng.choice(("0100755", "0100644", "0100600", "0120777"))
        line = (
            f"type=PATH msg=audit({ts_str}:{audit_id}): item=0 name=\"{path}\" "
            f"inode={inode} dev=fd:00 mode={mode} ouid=0 ogid=0 rdev=00:00 "
            f"nametype=NORMAL cap_fp=0 cap_fi=0 cap_fe=0 cap_fver=0 cap_frootid=0"
        )
        event_action = "path"
    elif kind == 4:
        argv = rng.choice(COMMANDS)
        line = (
            f"type=PROCTITLE msg=audit({ts_str}:{audit_id}): "
            f"proctitle={hex_proctitle(argv)}"
        )
        comm = argv[0]
        event_action = "proctitle"
    elif kind == 5:
        addr = f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
        res = rng.choice(("success", "failed"))
        line = (
            f"type=USER_LOGIN msg=audit({ts_str}:{audit_id}): pid={pid} uid={uid} "
            f"auid={auid} ses={ses} msg='op=login acct=\"{user}\" exe=\"{exe}\" "
            f"hostname=? addr={addr} terminal=ssh res={res}'"
        )
        event_action = "user-login"
    elif kind == 6:
        addr = f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
        terminal = rng.choice(("sshd", "sudo", "/dev/pts/0"))
        res = rng.choice(("success", "failed"))
        line = (
            f"type=USER_AUTH msg=audit({ts_str}:{audit_id}): pid={pid} uid={uid} "
            f"auid={auid} ses={ses} msg='op=PAM:authentication acct=\"{user}\" "
            f"exe=\"{exe}\" hostname={addr} addr={addr} terminal={terminal} res={res}'"
        )
        event_action = "user-auth"
    else:
        addr = f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
        port = rng.choice((22, 443, 8080, 9200))
        line = (
            f"type=SOCKADDR msg=audit({ts_str}:{audit_id}): "
            f"saddr={addr} laddr=127.0.0.1 lport={port}"
        )
        event_action = "sockaddr"

    ts_epoch = float(ts_str)
    doc = {
        "@timestamp": datetime.fromtimestamp(ts_epoch, tz=timezone.utc).isoformat(),
        "message": line,
        "event": {
            "dataset": DATASET,
            "module": "auditd",
            "category": ["authentication"] if "USER_" in line else ["process"],
            "type": ["start"] if event_action == "execve" else ["info"],
            "action": event_action,
        },
        "data_stream": {
            "type": "logs",
            "dataset": DATASET,
            "namespace": NAMESPACE,
        },
        "host": {"hostname": hostname, "name": hostname},
        "user": {"name": user, "id": str(uid)},
        "process": {"pid": pid, "parent": {"pid": ppid}, "name": comm, "executable": exe},
        "auditd": {
            "record_type": line.split(" ", 1)[0].removeprefix("type="),
            "sequence": audit_id,
            "session": ses,
        },
        "log": {"file": {"path": "/var/log/audit/audit.log"}},
    }
    return line, doc


def bulk_batch(docs: list[dict]) -> str:
    lines: list[str] = []
    for doc in docs:
        lines.append(json.dumps({"create": {"_index": DATA_STREAM}}))
        lines.append(json.dumps(doc))
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)
    base = datetime.now(timezone.utc) - timedelta(days=args.days_back)

    session = requests.Session()
    session.headers.update(auth_headers(args))
    verify = args.ca_cert if args.ca_cert else True

    # Warm up: ensure cluster reachable
    health = session.get(
        f"{args.host.rstrip('/')}/_cluster/health",
        auth=auth_tuple(args),
        verify=verify,
        timeout=30,
    )
    health.raise_for_status()
    print(f"Target: {args.host}")
    print(f"Data stream: {DATA_STREAM} ({args.count:,} documents, batch={args.batch_size:,})")

    sent = 0
    errors = 0
    t0 = time.perf_counter()

    while sent < args.count:
        n = min(args.batch_size, args.count - sent)
        batch_docs = []
        for i in range(n):
            _, doc = gen_audit_line(rng, sent + i + 1, base)
            batch_docs.append(doc)

        body = bulk_batch(batch_docs)
        r = session.post(
            f"{args.host.rstrip('/')}/_bulk",
            data=body,
            headers={"Content-Type": "application/x-ndjson"},
            auth=auth_tuple(args),
            verify=verify,
            timeout=120,
        )
        if not r.ok:
            print(f"Bulk HTTP error {r.status_code}: {r.text[:500]}", file=sys.stderr)
            return 1

        result = r.json()
        if result.get("errors"):
            for item in result.get("items", []):
                action = item.get("create") or item.get("index") or {}
                if "error" in action:
                    errors += 1
                    if errors <= 5:
                        print(f"  index error: {action['error']}", file=sys.stderr)
        sent += n
        elapsed = time.perf_counter() - t0
        rate = sent / elapsed if elapsed else 0
        print(f"  indexed {sent:>9,} / {args.count:,}  ({rate:,.0f} docs/s, errors={errors})", flush=True)

    elapsed = time.perf_counter() - t0
    print(f"\nDone: {sent:,} documents in {elapsed:.1f}s ({sent / elapsed:,.0f} docs/s)")
    print(f"Verify: GET {args.host}/{DATA_STREAM}/_count")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

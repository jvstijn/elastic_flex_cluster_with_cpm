#!/usr/bin/env python3
"""Fetch CPM watchers from Elasticsearch and write elastic_cpm Jinja2 templates.

Template styles match roles/elastic_cpm/templates conventions:
  plain  - no Jinja (cpm-scoring)
  simple - direct {{ webhook_host }} / {{ watcher_api_key.encoded }} (forecast-trigger)
  inline - {% raw %} with inline host/auth breaks (routing-advisor, register-sync)
  split  - host/auth split per webhook (pipeline-manager)
  scheme - scheme/host block per webhook (state-manager)
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import ssl
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

HOST_PLACEHOLDER = "YOUR_ES_HOST"
AUTH_PLACEHOLDER = "ApiKey YOUR_API_KEY"

DEFAULT_SPECS: list[dict[str, Any]] = [
    {"watch_id": "cpm-forecast-trigger", "template": "watcher_cpm-forecast-trigger.json.j2", "style": "simple", "alt_watch_ids": []},
    {"watch_id": "cpm-scoring", "template": "watcher_cpm-scoring.json.j2", "style": "plain", "alt_watch_ids": []},
    {"watch_id": "cpm-routing-advisor", "template": "watcher_cpm-routing-advisor.json.j2", "style": "inline", "alt_watch_ids": []},
    {
        "watch_id": "cpm-registry-sync",
        "template": "watcher_cpm-register-sync.json.j2",
        "style": "inline",
        "alt_watch_ids": ["cpm-register-sync"],
    },
    {"watch_id": "cpm-pipeline-manager", "template": "watcher_cpm-pipeline-manager.json.j2", "style": "split", "alt_watch_ids": []},
    {"watch_id": "cpm-state-manager", "template": "watcher_cpm-state-manager.json.j2", "style": "scheme", "alt_watch_ids": []},
]


def ssl_context(verify: bool) -> ssl.SSLContext | None:
    if verify:
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def request_json(
    url: str,
    *,
    verify_ssl: bool,
    api_key: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> dict:
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    elif user is not None and password is not None:
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, context=ssl_context(verify_ssl)) as resp:
        return json.load(resp)


def fetch_watch(
    base_url: str,
    watch_id: str,
    *,
    verify_ssl: bool,
    api_key: str | None,
    user: str | None,
    password: str | None,
) -> dict:
    data = request_json(
        f"{base_url.rstrip('/')}/_watcher/watch/{watch_id}",
        verify_ssl=verify_ssl,
        api_key=api_key,
        user=user,
        password=password,
    )
    if not data.get("found"):
        raise RuntimeError(f"watch {watch_id} not found")
    return data["watch"]


def resolve_webhook_host(es_url: str, watch: dict) -> str:
    host = None
    stack: list[Any] = [watch]
    while stack:
        obj = stack.pop()
        if isinstance(obj, dict):
            wh = obj.get("webhook")
            if isinstance(wh, dict) and wh.get("host"):
                host = str(wh["host"])
            stack.extend(obj.values())
        elif isinstance(obj, list):
            stack.extend(obj)
    if host:
        return host
    parsed = urlparse(es_url)
    return parsed.hostname or "localhost"


def sanitize_watch(watch: dict, webhook_host: str) -> dict:
    text = json.dumps(watch, ensure_ascii=False)
    text = text.replace(webhook_host, HOST_PLACEHOLDER)
    text = re.sub(
        r'"Authorization": "(?:ApiKey [A-Za-z0-9+/=]+|::es_redacted::)"',
        f'"Authorization": "{AUTH_PLACEHOLDER}"',
        text,
    )
    return json.loads(text)


def apply_scheme_auth(text: str) -> str:
    text = re.sub(
        r'(\s*"headers": \{\n)(\s*)"Authorization": "'
        + re.escape(AUTH_PLACEHOLDER)
        + r'",\n(\s*)"Content-Type":',
        r'\1{% endraw %}\n\2"Authorization": "Apikey {{ watcher_api_key.encoded  }}",\n{% raw %}\n\3"Content-Type":',
        text,
    )
    text = re.sub(
        r'(\s*"headers": \{\n)(\s*)"Authorization": "'
        + re.escape(AUTH_PLACEHOLDER)
        + r'"\n(\s*\}),',
        r'\1{% endraw %}\n\2"Authorization": "Apikey {{ watcher_api_key.encoded  }}"\n{% raw %}\n\3,',
        text,
    )
    return text


def apply_inline(text: str) -> str:
    text = text.replace(
        f'"host": "{HOST_PLACEHOLDER}"',
        '"host": "{% endraw %}{{ webhook_host }}{% raw %}"',
    )
    text = text.replace(
        f'"Authorization": "{AUTH_PLACEHOLDER}"',
        '"Authorization": "ApiKey {% endraw %}{{ watcher_api_key.encoded }}{% raw %}"',
    )
    return text


def apply_split(text: str) -> str:
    text = re.sub(
        r'("scheme": "https",)\n(\s*)"host": "' + re.escape(HOST_PLACEHOLDER) + r'",',
        r'\1\n{% endraw %}\n\2"host": "{{ webhook_host }}",\n{% raw %}',
        text,
    )
    return apply_scheme_auth(text)


def apply_scheme(text: str) -> str:
    text = re.sub(
        r'(\s*)"webhook": \{\n\1        "scheme": "https",\n\1        "host": "'
        + re.escape(HOST_PLACEHOLDER)
        + r'",',
        r'\1"webhook": {\n{% endraw %}\n\1        "scheme": "https",\n\1        "host": "{{ webhook_host }}",\n{% raw %}',
        text,
    )
    return apply_scheme_auth(text)


def to_plain(watch: dict) -> str:
    return json.dumps(watch, indent=2, ensure_ascii=False) + "\n"


def to_simple(watch: dict) -> str:
    text = json.dumps(watch, indent=2, ensure_ascii=False)
    text = text.replace(f'"host": "{HOST_PLACEHOLDER}"', '"host": "{{ webhook_host }}"')
    text = text.replace(
        f'"Authorization": "{AUTH_PLACEHOLDER}"',
        '"Authorization": "Apikey {{ watcher_api_key.encoded  }}"',
    )
    return text + "\n"


def to_raw_template(watch: dict, style: str) -> str:
    text = json.dumps(watch, indent=2, ensure_ascii=False)
    if style == "inline":
        text = apply_inline(text)
    elif style == "split":
        text = apply_split(text)
    elif style == "scheme":
        text = apply_scheme(text)
    else:
        raise ValueError(f"unknown raw style: {style}")
    return "{% raw %}\n" + text + "\n{% endraw %}\n"


def convert(watch: dict, style: str) -> str:
    if style == "plain":
        return to_plain(watch)
    if style == "simple":
        return to_simple(watch)
    return to_raw_template(watch, style)


def load_specs(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return DEFAULT_SPECS
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("watchers"), list):
        return data["watchers"]
    raise ValueError(f"invalid watch spec file: {path}")


def export_watchers(
    *,
    base_url: str,
    output_dir: Path,
    specs: list[dict[str, Any]],
    verify_ssl: bool,
    api_key: str | None,
    user: str | None,
    password: str | None,
    write_json: bool,
) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_dir = output_dir.parent / "exported_json" if write_json else None
    if json_dir is not None:
        json_dir.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    for spec in specs:
        watch_id = spec["watch_id"]
        template_name = spec["template"]
        style = spec.get("style", "inline")
        candidates = [watch_id, *spec.get("alt_watch_ids", [])]

        watch = None
        resolved_id = None
        last_error: Exception | None = None
        for candidate in candidates:
            try:
                watch = fetch_watch(
                    base_url,
                    candidate,
                    verify_ssl=verify_ssl,
                    api_key=api_key,
                    user=user,
                    password=password,
                )
                resolved_id = candidate
                break
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code == 404:
                    continue
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                break

        if watch is None:
            print(f"  skip {watch_id}: {last_error}", file=sys.stderr)
            continue

        webhook_host = resolve_webhook_host(base_url, watch)
        sanitized = sanitize_watch(watch, webhook_host)
        rendered = convert(sanitized, style)

        out_path = output_dir / template_name
        out_path.write_text(rendered, encoding="utf-8")
        written.append(template_name)
        print(f"  exported {resolved_id} -> {out_path}")

        if json_dir is not None:
            json_path = json_dir / f"{watch_id}.json"
            json_path.write_text(json.dumps(sanitized, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    return written


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--watch-spec", type=Path, help="JSON file with watcher export specs")
    parser.add_argument("--verify-ssl", action="store_true")
    parser.add_argument("--write-json", action="store_true", help="Also write sanitized JSON next to templates")
    auth = parser.add_mutually_exclusive_group()
    auth.add_argument("--api-key")
    auth.add_argument("--basic-auth", nargs=2, metavar=("USER", "PASSWORD"))
    args = parser.parse_args()

    api_key = args.api_key
    user = password = None
    if args.basic_auth:
        user, password = args.basic_auth

    if not api_key and user is None:
        print("Error: pass --api-key or --basic-auth USER PASSWORD", file=sys.stderr)
        return 2

    specs = load_specs(args.watch_spec)
    written = export_watchers(
        base_url=args.url,
        output_dir=args.output_dir.resolve(),
        specs=specs,
        verify_ssl=args.verify_ssl,
        api_key=api_key,
        user=user,
        password=password,
        write_json=args.write_json,
    )
    if not written:
        print("No watchers exported.", file=sys.stderr)
        return 1
    print(f"OK: {len(written)} template(s) written to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

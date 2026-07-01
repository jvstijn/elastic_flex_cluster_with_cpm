#!/usr/bin/env python3
"""Export cpm_configs.json artifacts into elastic_cpm role files."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CPM_CONFIGS = ROOT.parent.parent.parent / "cpm" / "cpm_configs.json"
ROLE_FILES = ROOT / "roles" / "elastic_cpm" / "files" / "json"

INDEX_SETTINGS = {"number_of_shards": 1, "number_of_replicas": 1}


def main() -> None:
    with CPM_CONFIGS.open() as f:
        bundle = json.load(f)

    ROLE_FILES.mkdir(parents=True, exist_ok=True)

    for idx, mapping in bundle.get("mappings", {}).items():
        path = ROLE_FILES / f"index_{idx}.json"
        path.write_text(
            json.dumps({"settings": INDEX_SETTINGS, "mappings": mapping}, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Wrote {path.name}")

    for name, cfg in bundle.get("ingest_pipelines", {}).items():
        path = ROLE_FILES / f"ingest_pipeline_{name}.json"
        path.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {path.name}")

    for name, cfg in bundle.get("transforms", {}).items():
        path = ROLE_FILES / f"transform_{name}.json"
        path.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {path.name}")

    global_doc = bundle.get("routing_config_defaults", {}).get("_global", {})
    if global_doc:
        path = ROLE_FILES / "routing_config_global.json"
        path.write_text(json.dumps(global_doc, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {path.name}")

    for name, cfg in bundle.get("jobs", {}).items():
        path = ROLE_FILES / f"ml_anomaly_detectors_{name}.json"
        path.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {path.name}")

    for name, cfg in bundle.get("feeds", {}).items():
        cfg = dict(cfg)
        cfg.pop("authorization", None)
        cfg["job_id"] = name.replace("datafeed-", "", 1)
        path = ROLE_FILES / f"ml_datafeed_{name}.json"
        path.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {path.name}")

    for tpl_id, tpl in bundle.get("templates", {}).items():
        path = ROLE_FILES / f"cpm-pipeline-template-{tpl_id}.json"
        path.write_text(json.dumps(tpl, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {path.name}")


if __name__ == "__main__":
    main()

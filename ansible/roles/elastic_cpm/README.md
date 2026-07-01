# elastic_cpm Ansible role

Deploys the full CPM stack on central Elasticsearch:

- Monitoring field probes (auto-detect Metricbeat field paths)
- 12 `cpm-*` config indices
- 5 ML jobs + datafeeds
- Ingest pipeline `cpm-watcher-status-flatten` + transform `cpm-watcher-status-sync`
- 6 Watcher definitions
- Pipeline templates and `_global` routing weights

See [../../README.md](../../README.md) for usage.

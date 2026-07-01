# CPM Ansible deployment

Installs the full Cluster Pipeline Manager stack on **es-central** via the Elasticsearch API.

Replaces `cpm/cpm_install.py` and `scripts/bootstrap_cpm_pipelines.py`.

## Prerequisites

- Running Elasticsearch central cluster with Stack Monitoring data (`.monitoring-es-8-*`)
- `elastic` user password in `../.env` (`ELASTIC_PASSWORD`, `ES_CENTRAL_PORT`)
- Ansible 2.14+ on the docker host

## Quick start

```bash
cd Jan/elastic_flex_cluster_with_cpm/ansible

# Full CPM install (ML, 12 indices, ingest pipeline, transform, 6 watchers)
ansible-playbook site.yml

# First-time routing + Logstash pipeline push
ansible-playbook bootstrap.yml
```

## Playbooks

| Playbook | Purpose |
|----------|---------|
| `site.yml` | Core CPM install via `elastic_cpm` role |
| `bootstrap.yml` | Execute watcher chain, seed catchall state, push pipelines |
| `workflows.yml` | Optional Elastic 9.4+ workflow port (not default) |
| `extract_watches.yml` | Dev tool: export live watchers to `.j2` templates |

## Tags

```bash
ansible-playbook site.yml --tags probe      # monitoring field detection only
ansible-playbook site.yml --tags indices    # config indices
ansible-playbook site.yml --tags ml         # ML jobs + datafeeds
ansible-playbook site.yml --tags transform  # ingest pipeline + transform
ansible-playbook site.yml --tags watchers   # API key + 6 watchers
ansible-playbook site.yml --tags seed       # templates + routing _global
ansible-playbook site.yml --tags registry   # patch ingest_hosts / dc
ansible-playbook site.yml --tags ml_reinstall  # with -e cpm_ml_reinstall=true
ansible-playbook site.yml --tags clean -e cpm_clean_indices=true  # destructive
```

## Variables (`group_vars/all.yml`)

| Variable | Default | Description |
|----------|---------|-------------|
| `elastic_base_url` | `https://localhost:{{ es_central_port }}` | Central ES API |
| `webhook_host` | `es-central` | Hostname watchers use inside Docker network |
| `cpm_monitoring_index` | `.monitoring-es-8-*` | Monitoring index pattern |
| `cpm_ml_reinstall` | `false` | Delete and recreate ML jobs/datafeeds |
| `cpm_clean_indices` | `false` | Delete all `cpm-*` indices before create |
| `cpm_datafeed_start` | _(empty → now-2d)_ | ML datafeed start timestamp |
| `cpm_cluster_registry` | 3-cluster docker map | `ingest_hosts` / `dc` patches |

## Verify

```bash
curl -sk -u elastic:$ELASTIC_PASSWORD https://localhost:$ES_CENTRAL_PORT/_ml/anomaly_detectors/cpm-store-size
curl -sk -u elastic:$ELASTIC_PASSWORD https://localhost:$ES_CENTRAL_PORT/_watcher/watch/cpm-registry-sync
curl -sk -u elastic:$ELASTIC_PASSWORD https://localhost:$ES_CENTRAL_PORT/cpm-routing-config/_doc/_global
curl -sk -u elastic:$ELASTIC_PASSWORD https://localhost:$ES_CENTRAL_PORT/_transform/cpm-watcher-status-sync/_stats
```

## Regenerate JSON artifacts from `cpm_configs.json`

```bash
python3 scripts/export_cpm_configs.py
```

Run after editing [cpm/cpm_configs.json](../../../cpm/cpm_configs.json).

## Required Elasticsearch privileges

The `elastic` superuser is used by Ansible. Watchers receive a dedicated `cpm-watcher-webhook` API key with broad index/cluster privileges.

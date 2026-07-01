# CPM Ansible deployment

Installs the full Cluster Pipeline Manager stack on **es-central** via the Elasticsearch API.

## Inventories

| Inventory | Use case |
|-----------|----------|
| `inventories/kaposi` **(default)** | imr-dod-vm reference stack via public nginx (`central.kaposi.net`, `cpm.kaposi.net`) |
| `inventories/local` | Jan 3-cluster docker-compose demo on localhost |

Default in `ansible.cfg` is **kaposi**. Override:

```bash
ansible-playbook -i inventories/local site.yml
```

### kaposi.net (reference environment)

| Endpoint | URL |
|----------|-----|
| Central Elasticsearch | `https://central.kaposi.net` |
| Kibana (+ API) | `https://cpm.kaposi.net` |
| Remote clusters | `https://cluster01.kaposi.net` … `cluster15.kaposi.net` |

Credentials are read from [`docker/reference/.env`](../../../docker/reference/.env) (`ELASTIC_PASSWORD`, `KIBANA_PASSWORD`). Copy that file locally before running Ansible from your laptop, or run playbooks on `imr-dod-vm` where it already exists.

Kibana API tasks (dashboard tag) authenticate as **`elastic`** using `ELASTIC_PASSWORD`. `KIBANA_PASSWORD` is the `kibana_system` password for the Kibana container only.

`webhook_host` stays **`es-central-01`** (internal docker DNS) — watchers execute on the ES node and call back on port 9200 inside the stack.

## Workstation setup (run once)

Use the **ansible** venv here — not `cpm/.venv`:

```bash
cd Jan/elastic_flex_cluster_with_cpm/ansible

# copy credentials from VM (one time)
scp imr-dod-vm:~/DoD/docker/reference/.env .env

# create .venv and install ansible
./scripts/setup_local.sh

source .venv/bin/activate
./scripts/connectivity_test.sh
ansible-playbook site.yml
ansible-playbook bootstrap.yml
```

Credentials live in `ansible/.env` (gitignored). Override path: `export CPM_ENV_FILE=/path/to/.env`

## Pre-flight connectivity test

```bash
# Shell (fastest)
export ELASTIC_PASSWORD=...   # or copy docker/reference/.env
./scripts/connectivity_test.sh

# Ansible (same checks)
ansible-playbook connectivity_test.yml
```

Tests: ES health/license/monitoring/ML/Watcher/Transform APIs, Kibana `/api/status`, `/api/spaces/space`, `/api/saved_objects/_find`, and remote `cluster05`.

Kibana API is exposed through nginx on `cpm.kaposi.net` (all paths proxy to Kibana `:5601`); no extra nginx vhost is required.

## Quick start

```bash
cd Jan/elastic_flex_cluster_with_cpm/ansible
./scripts/connectivity_test.sh
ansible-playbook site.yml
ansible-playbook bootstrap.yml
```

## Playbooks

| Playbook | Purpose |
|----------|---------|
| `connectivity_test.yml` | Pre-flight API checks |
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
ansible-playbook site.yml --tags dashboard  # Kibana saved objects (4 dashboards)
ansible-playbook site.yml --tags ml_reinstall -e cpm_ml_reinstall=true
ansible-playbook site.yml --tags clean -e cpm_clean_indices=true  # destructive
```

## Variables (`inventories/kaposi/group_vars/all.yml`)

| Variable | kaposi value | Description |
|----------|--------------|-------------|
| `elastic_base_url` | `https://central.kaposi.net` | Central ES API (nginx :443) |
| `kibana_base_url` | `https://cpm.kaposi.net` | Kibana UI + API |
| `cpm_validate_certs` | `true` | Let's Encrypt TLS verification |
| `cpm_env_file` | `docker/reference/.env` | Credentials source |
| `webhook_host` | `es-central-01` | Internal watcher callback host |
| `cpm_cluster_registry` | 16 clusters | `ingest_hosts` / `dc` per cluster |

## Regenerate JSON artifacts from `cpm_configs.json`

```bash
python3 scripts/export_cpm_configs.py
```

## Regenerate Kibana dashboard saved objects

After changing `cpm/scripts/deploy_cpm_dashboard.py`:

```bash
python3 ../../../cpm/scripts/export_cpm_dashboard_objects.py
```

Writes `roles/elastic_cpm/files/kibana/` (data views, visualizations, searches, dashboards).
Deploy with `ansible-playbook site.yml --tags dashboard` or as part of full `site.yml`.

Main dashboard: `https://cpm.kaposi.net/app/dashboards#/view/cpm-platform-overview` (kaposi inventory).

## Required Elasticsearch privileges

The `elastic` superuser is used by Ansible. Watchers receive a dedicated `cpm-watcher-webhook` API key with broad index/cluster privileges.

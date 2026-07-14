# CPM Ansible deployment

Installs the full Cluster Pipeline Manager stack on **es-central** via the Elasticsearch API.

## Inventories

| Inventory | Use case |
|-----------|----------|
| `inventories/kaposi` **(default)** | imr-dod-vm reference stack via public nginx (`central.kaposi.net`, `cpm.kaposi.net`) |
| `inventories/local` | 3-cluster demo — `Jan/docker-local` + Ansible on localhost |

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

### Stack monitoring prerequisite (Metricbeat → `.monitoring-es-8-*`)

CPM ML jobs, watchers, and field probes read **Metricbeat** monitoring indices on central — not legacy internal collection (`.monitoring-es-7-*`).

On **imr-dod-vm** (once, or after stack changes):

```bash
cd ~/DoD/docker/reference
./scripts/enable_stack_monitoring.sh
```

Verify locally: `./scripts/connectivity_test.sh` (fails if es-8 monitoring is empty).

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

Tests: ES health/license/monitoring/ML/Watcher/Transform APIs, **nginx hostname → cluster_name routing**, Kibana `/api/status`, `/api/spaces/space`, `/api/saved_objects/_find`, and remote `cluster05`.

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
ansible-playbook site.yml --tags ml         # ML jobs + datafeeds (open jobs, start datafeeds continuously)
ansible-playbook site.yml --tags transform  # ingest pipeline + transform
ansible-playbook site.yml --tags watchers   # API key + 7 watchers
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
| `kibana_url` | `https://cpm.kaposi.net` | Kibana UI + API |
| `cpm_validate_certs` | `true` | Let's Encrypt TLS verification |
| `cpm_env_file` | `docker/reference/.env` | Credentials source |
| `webhook_host` | `es-central-01` | Internal watcher callback host |
| `cpm_cluster_registry` | 16 clusters | `ingest_hosts` / `dc` per cluster |
| `cpm_datafeed_start` | `""` (auto `now-2d`) | Backfill start when starting a stopped datafeed |

## ML jobs — continuous (real-time) operation

CPM expects all five ML jobs to stay **opened** with datafeeds **started** indefinitely (Kibana: “continuous” / real-time). That is what `_open` + `POST _ml/datafeeds/<id>/_start` with only a `start` timestamp (no `end`) does.

Ansible handles this in `roles/elastic_cpm/tasks/ml.yml` when you run:

```bash
ansible-playbook site.yml --tags ml
```

On each run it will:

1. Create jobs/datafeeds if missing (or recreate when `cpm_ml_reinstall=true`)
2. **Open** any closed jobs
3. **Start** any stopped datafeeds from `cpm_datafeed_start` (default: 2 days ago)

Already-running datafeeds are left alone. After an Elasticsearch restart or a failed job, re-run `--tags ml` instead of using the Kibana UI.

`cpm_install.py` does the same (open + start); prefer Ansible for kaposi so ML stays in sync with the role JSON artifacts.

## Watchers

| Watcher | Schedule (UTC) | Purpose |
|---------|----------------|---------|
| `cpm-registry-sync` | daily | Sync `cpm-cluster-registry` from Stack Monitoring |
| `cpm-forecast-trigger` | daily | Refresh ML forecasts (optional before scoring) |
| `cpm-scoring` | daily | Write cluster scores |
| `cpm-routing-advisor` | daily | Routing suggestions |
| `cpm-state-manager` | daily | Desired state → `cpm-pipeline-state` |
| `cpm-pipeline-manager` | `0 20 0 * * ?` | Push Logstash pipelines |
| `cpm-stream-coverage` | `0 25 0 * * ?` | Stream vs pipeline coverage → `cpm-stream-coverage` |

**`cpm-stream-coverage`** runs five minutes after pipeline-manager. It:

1. Reads Stack Monitoring index stats (**1h window**) per cluster and backing index
2. Treats a stream as active when `index_total` increased in that window; stores the delta as `writes_detected`
3. Maps each backing index to a data-stream name and Kafka topic (`logs-dataset-ns`, or `filebeat`)
4. Loads topic lists from `GET /_logstash/pipeline` and optional `cpm-pipeline-state`
5. Clears `cpm-stream-coverage`, then bulk-indexes one document per `{cluster_id}|{type|dataset|namespace}`

Dashboard panels on **Platform Overview** (`cpm-search-stream-coverage`, managed/unmanaged metrics) read this index. Each run reflects **writes in the hour before execution** (daily at 00:25 UTC, or manual). They stay empty until this watcher runs (bootstrap executes it after pipeline-manager). For ad-hoc checks on a single cluster, use `scripts/check_index_pipeline_coverage.py`.

Manual run:

```bash
curl -u elastic -X POST "$ES/_watcher/watch/cpm-stream-coverage/_execute" \
  -H 'Content-Type: application/json' -d '{"record_execution":true}'
```

**Kibana plugin:** Changes to the Run CPM chain UI (`kibana_plugin/cpm/`) are not deployed by `--tags watchers`. After plugin source changes, rebuild and reinstall on Kibana:

```bash
chmod +x scripts/build_kibana_cpm_plugin.sh
./scripts/build_kibana_cpm_plugin.sh
# install kibana_plugin/build/cpm-8.19.16.zip on cpm.kaposi.net (see kibana_plugin/README.md)
```

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

## Kibana CPM plugin (Stack Management → Ingest)

Source: `../kibana_plugin/` — management UI for registry, scoring weights, stream locks, and watcher execution.

```bash
# Build plugin zip (first run clones Kibana v8.19.16)
chmod +x ../scripts/build_kibana_cpm_plugin.sh
../scripts/build_kibana_cpm_plugin.sh

# Or build custom Kibana image for docker/reference
cd ../kibana_plugin && docker build -t kibana-cpm:8.19.16 .
# Set KIBANA_IMAGE=kibana-cpm:8.19.16 in docker/reference/.env
```

UI path: **Stack Management → Ingest → Cluster Pipeline Manager**

Requires a Kibana user with read/write on `cpm-cluster-registry` and `cpm-routing-config`, plus `manage_watcher` for the Run CPM tab. The management UI itself requires any of the cluster privileges `monitor`, `manage`, `manage_pipeline`, or `manage_logstash_pipelines`.

## Required Elasticsearch privileges

The `elastic` superuser is used by Ansible. Watchers receive a dedicated `cpm-watcher-webhook` API key with broad index/cluster privileges.

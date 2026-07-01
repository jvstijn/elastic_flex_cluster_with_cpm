# CPM Kibana plugin

Management UI under **Stack Management → Ingest → Cluster Pipeline Manager**.

Targets Kibana **8.19.16** (must match `STACK_VERSION` in docker/reference).

## Features

| Tab | Index / API | Action |
|-----|-------------|--------|
| **Clusters** | `cpm-cluster-registry` | Enable/disable clusters (`active` flag) |
| **Scoring weights** | `cpm-routing-config/_global` | Edit disk / jvm / shard / load weights, alert threshold |
| **Stream locks** | `cpm-routing-config` (`stream_lock`) | Wizard to add locks; delete; apply state+pipeline managers |
| **Run CPM** | `/_watcher/watch/*/_execute` | Run full watcher chain or individual watchers |

Watcher chain order (matches Ansible bootstrap):

1. `cpm-registry-sync` (optional: `cpm-forecast-trigger` before scoring)
2. `cpm-scoring`
3. `cpm-routing-advisor`
4. `cpm-state-manager`
5. `cpm-pipeline-manager`

## Build

Plugins must be compiled against the matching Kibana version.

```bash
cd Jan/elastic_flex_cluster_with_cpm
chmod +x scripts/build_kibana_cpm_plugin.sh
./scripts/build_kibana_cpm_plugin.sh
```

Output: `kibana_plugin/build/cpm-8.19.16.zip`

### Docker image (reference stack)

```bash
cd Jan/elastic_flex_cluster_with_cpm/kibana_plugin
docker build -t kibana-cpm:8.19.16 .
```

Set in `docker/reference/.env`:

```bash
KIBANA_IMAGE=kibana-cpm:8.19.16
```

Then `docker compose up -d kibana`.

### Manual install into running Kibana

```bash
docker compose cp kibana_plugin/build/cpm-8.19.16.zip kibana:/tmp/cpm.zip
docker compose exec kibana bin/kibana-plugin install file:///tmp/cpm.zip
docker compose restart kibana
```

## Permissions

Server routes use `elasticsearch.client.asCurrentUser`. The logged-in Kibana user needs:

- Read/write on `cpm-cluster-registry`, `cpm-routing-config`
- `manage_watcher` (or superuser) to execute watchers

The `elastic` superuser satisfies these requirements.

## Development

```bash
export KIBANA_DIR=~/src/kibana   # existing v8.19.16 checkout
./scripts/build_kibana_cpm_plugin.sh
```

For live reload, run `yarn dev` in `plugins/cpm` and `yarn start` in the Kibana root (see Elastic external plugin docs).

## Layout

```
kibana_plugin/cpm/
  kibana.json          # plugin manifest
  public/              # React UI (management ingest section)
  server/routes/       # /api/cpm/* proxies to Elasticsearch
  common/              # shared constants and types
```

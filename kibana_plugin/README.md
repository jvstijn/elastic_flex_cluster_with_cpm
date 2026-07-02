# CPM Kibana plugin

Management UI under **Stack Management → Ingest → Cluster Pipeline Manager**.

Targets Kibana **8.19.16** (must match the Kibana version on the host — e.g. `STACK_VERSION` in `docker/reference`).

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

---

## Building the plugin (CI/CD and dedicated Kibana)

Kibana plugins are **compiled against a specific Kibana release**. The build does **not** run on the Kibana server itself — a CI/CD agent (or build VM) produces a **zip artifact** that platform ops install on each Kibana node.

### Output artifact

| Item | Value |
|------|--------|
| **File** | `kibana_plugin/build/cpm-<kibanaVersion>.zip` (e.g. `cpm-8.19.16.zip`) |
| **Format** | Standard Kibana plugin zip (same as `bin/kibana-plugin install`) |
| **Source** | `kibana_plugin/cpm/` only — there is no separate npm project at `kibana_plugin/` root |

Treat the zip as the **release artifact**: publish it from CI (Artifactory, S3, pipeline artifacts, etc.) and install the same build on every Kibana instance in an environment.

### What the build agent needs

Platform engineers should provision build runners with:

| Requirement | Notes |
|-------------|--------|
| **OS** | Linux x86_64 recommended (matches typical Kibana deployments) |
| **Git** | Shallow clone of `https://github.com/elastic/kibana.git` at tag `v<KIBANA_VERSION>` |
| **Node.js** | **22.x** (see `kibana_plugin/Dockerfile` — `node:22.22.2`) |
| **Yarn** | Via **Corepack** (`corepack enable`); Kibana uses its own Yarn version after bootstrap |
| **Build tools** | `python3`, `build-essential` (or equivalent) — required by Kibana bootstrap native deps |
| **Disk** | **~8 GB** free for Kibana checkout + `node_modules` (first build); less if cache is warm |
| **Network** | Outbound HTTPS to `github.com` (Kibana source) and npm registries during bootstrap |
| **Memory** | **≥ 8 GB RAM** recommended for `yarn kbn bootstrap` |

No Kibana process needs to run on the builder — only the compile toolchain.

### Build command

From the repository root (`Jan/elastic_flex_cluster_with_cpm/`):

```bash
chmod +x scripts/build_kibana_cpm_plugin.sh
./scripts/build_kibana_cpm_plugin.sh
```

The script:

1. Clones Kibana `v${KIBANA_VERSION}` into `.kibana-build/` (or `$KIBANA_DIR`)
2. Runs `yarn kbn bootstrap --skip-os-packages` once per checkout
3. Copies `kibana_plugin/cpm` into `plugins/cpm` inside that checkout
4. Runs `yarn build` in the plugin directory
5. Copies the resulting zip to `kibana_plugin/build/`

### Environment variables (CI/CD)

| Variable | Default | Purpose |
|----------|---------|---------|
| `KIBANA_VERSION` | `8.19.16` | Must match target Kibana and `cpm/kibana.json` → `kibanaVersion` |
| `KIBANA_DIR` | `<repo>/.kibana-build` | Path to Kibana git checkout; **cache this directory** between pipeline runs |

Example pipeline step:

```bash
export KIBANA_VERSION=8.19.16
export KIBANA_DIR="${CI_PROJECT_DIR}/.cache/kibana-${KIBANA_VERSION}"
./scripts/build_kibana_cpm_plugin.sh
```

**Caching:** Persist `$KIBANA_DIR` and its `node_modules/` between builds. The first run takes several minutes (clone + bootstrap); incremental runs only re-copy plugin sources and rebuild the zip (~1–2 min).

**Version bumps:** When upgrading Kibana, update all of:

- `KIBANA_VERSION` / `STACK_VERSION`
- `kibana_plugin/cpm/kibana.json` → `kibanaVersion`
- `kibana_plugin/cpm/package.json` → `kibana.version`
- Clear or replace the cached `$KIBANA_DIR` for the new tag

### CI/CD pipeline sketch

```yaml
# Pseudocode — adapt to GitLab / GitHub Actions / Jenkins
build-cpm-plugin:
  image: node:22-bookworm   # or use kibana_plugin/Dockerfile builder stage
  cache:
    key: kibana-v8.19.16
    paths:
      - .cache/kibana-8.19.16/
  script:
    - apt-get update && apt-get install -y git python3 build-essential
    - export KIBANA_VERSION=8.19.16
    - export KIBANA_DIR=$CI_PROJECT_DIR/.cache/kibana-8.19.16
    - ./scripts/build_kibana_cpm_plugin.sh
  artifacts:
    paths:
      - kibana_plugin/build/cpm-*.zip
    expire_in: 30 days
```

Downstream deploy jobs download the zip and install it on Kibana hosts (see below). Do **not** commit `kibana_plugin/build/` or `.kibana-build/` — both are gitignored.

### Install on a dedicated Kibana machine

After CI publishes `cpm-8.19.16.zip`:

```bash
# Copy artifact to the Kibana host, then as root or kibana user:
sudo -u kibana /usr/share/kibana/bin/kibana-plugin install file:///path/to/cpm-8.19.16.zip
sudo systemctl restart kibana
```

Docker / Compose equivalent:

```bash
docker compose cp kibana_plugin/build/cpm-8.19.16.zip kibana:/tmp/cpm.zip
docker compose exec kibana bin/kibana-plugin install file:///tmp/cpm.zip
docker compose restart kibana
```

Verify: **Stack Management → Ingest → Cluster Pipeline Manager** appears after restart.

**Rolling upgrades:** Build one zip per Kibana minor version; install the matching zip on each node before or during the Kibana upgrade window. Plugin `kibanaVersion` must exactly match the running Kibana build.

---

## Reference stack: Docker image with plugin baked in

For the local / kaposi reference environment you can bake the plugin into a custom Kibana image instead of installing the zip manually:

```bash
cd Jan/elastic_flex_cluster_with_cpm/kibana_plugin
docker build -t kibana-cpm:8.19.16 .
```

Set in `docker/reference/.env`:

```bash
KIBANA_IMAGE=kibana-cpm:8.19.16
```

Then `docker compose up -d kibana`.

The Dockerfile uses the same Kibana clone + `yarn build` flow as `scripts/build_kibana_cpm_plugin.sh`, then runs `bin/kibana-plugin install` in the final image layer.

---

## Permissions

Server routes use `elasticsearch.client.asCurrentUser`. The logged-in Kibana user needs:

- Read/write on `cpm-cluster-registry`, `cpm-routing-config`
- `manage_watcher` (or superuser) to execute watchers

The `elastic` superuser satisfies these requirements.

---

## Development

For interactive UI work, use an existing Kibana **8.19.16** source checkout:

```bash
export KIBANA_DIR=~/src/kibana   # must be v8.19.16
./scripts/build_kibana_cpm_plugin.sh
```

For live reload, symlink or copy `kibana_plugin/cpm` into `<kibana>/plugins/cpm`, then run `yarn dev` in the plugin directory and `yarn start` in the Kibana root (see [Elastic external plugin docs](https://www.elastic.co/docs/extend/kibana)).

---

## Layout

```
kibana_plugin/
  cpm/                 # plugin source (only directory that matters for builds)
    kibana.json        # plugin manifest (id, kibanaVersion)
    package.json       # yarn build scripts (uses Kibana's plugin_helpers)
    public/            # React UI (management ingest section)
    server/routes/     # /api/cpm/* proxies to Elasticsearch
    common/            # shared constants and types
  build/               # output zips (gitignored, CI artifact)
  Dockerfile           # optional: Kibana image with plugin pre-installed
```

Build script: `scripts/build_kibana_cpm_plugin.sh` (repo root).

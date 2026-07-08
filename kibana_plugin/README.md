# CPM Kibana plugin

Management UI: **Stack Management → Ingest → Cluster Pipeline Manager**.

| Tab | Index / API | Action |
|-----|-------------|--------|
| **Clusters** | `cpm-cluster-registry` | Enable/disable clusters (`active`) |
| **Scoring weights** | `cpm-routing-config/_global` | Disk / JVM / shard / load weights, alert threshold |
| **Stream locks** | `cpm-routing-config` | Pin streams to clusters |
| **Run CPM** | `/_watcher/watch/*/_execute` | Run watcher chain or individual watchers |

Watcher chain: `cpm-registry-sync` → `cpm-scoring` → `cpm-routing-advisor` → `cpm-state-manager` → `cpm-pipeline-manager` → `cpm-stream-coverage`.

---

## Build the plugin (step by step)

The plugin **must** be compiled against the **exact** Kibana version running in production.  
Output: `kibana_plugin/build/cpm-<version>.zip` (standard `bin/kibana-plugin install` format).

Source lives in `kibana_plugin/cpm/` only.

### Prerequisites

| Tool | Notes |
|------|--------|
| Git | shallow clone of `elastic/kibana` |
| Node.js **22.x** | e.g. 22.22.2 (`Dockerfile` uses `node:22.22.2-bookworm-slim`) |
| Corepack + Yarn | `corepack enable` |
| `python3`, C build tools | `build-essential` on Debian/Ubuntu |
| ~8 GB disk | first clone + bootstrap |
| ~8 GB RAM | recommended for `yarn kbn bootstrap` |

No Kibana process runs on the builder — only the compile toolchain.

### Step 1 — Pick the Kibana version

| Target Kibana | Git tag | Zip output |
|---------------|---------|------------|
| **8.19.16** (current) | `v8.19.16` | `cpm-8.19.16.zip` |
| **9.4.3** (example) | `v9.4.3` | `cpm-9.4.3.zip` |

When switching versions, update both manifest fields in `cpm/`:

- `kibana.json` → `kibanaVersion`
- `package.json` → `kibana.version`

### Step 2 — Build with the repo script (recommended)

From `Jan/elastic_flex_cluster_with_cpm/`:

```bash
chmod +x scripts/build_kibana_cpm_plugin.sh

# 8.19.16 (default)
./scripts/build_kibana_cpm_plugin.sh

# 9.4.3
KIBANA_VERSION=9.4.3 ./scripts/build_kibana_cpm_plugin.sh
```

The script:

1. Clones Kibana `v${KIBANA_VERSION}` into `.kibana-build/` (or `$KIBANA_DIR`)
2. Runs `yarn kbn bootstrap --skip-os-packages` once per checkout
3. Copies `kibana_plugin/cpm` → `plugins/cpm`
4. Runs `yarn build` in the plugin directory
5. Copies the zip to `kibana_plugin/build/`

**Cache between builds** (bootstrap runs only once per version):

```bash
KIBANA_VERSION=9.4.3 \
KIBANA_DIR=$HOME/.cache/kibana-9.4.3 \
./scripts/build_kibana_cpm_plugin.sh
```

| Variable | Default | Purpose |
|----------|---------|---------|
| `KIBANA_VERSION` | `8.19.16` | Must match `cpm/kibana.json` → `kibanaVersion` |
| `KIBANA_DIR` | `<repo>/.kibana-build` | Kibana git checkout — persist in CI |

Do **not** commit `kibana_plugin/build/` or `.kibana-build/` (gitignored).

### Step 2b — Build zip in Docker (artifact on host)

No local Node/Yarn/fnm required. Output: `kibana_plugin/build/cpm-<version>.zip`.

From `Jan/elastic_flex_cluster_with_cpm/`:

```bash
chmod +x scripts/build_kibana_cpm_plugin_docker.sh
./scripts/build_kibana_cpm_plugin_docker.sh

# other Kibana version
KIBANA_VERSION=9.4.3 ./scripts/build_kibana_cpm_plugin_docker.sh
```

Manual equivalent:

```bash
cd kibana_plugin
docker build -f Dockerfile.build -t cpm-kibana-plugin-build:8.19.16 .
docker run --rm -v "$(pwd)/build:/output" cpm-kibana-plugin-build:8.19.16
ls -la build/cpm-8.19.16.zip
```

First `docker build` is slow (~15–20 min); Docker layer cache speeds up rebuilds when only `cpm/` source changes.

Playwright browser downloads are skipped (`PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1`) — not needed for plugin zips.

Bootstrap runs `yarn kbn build-shared` to warm Moon/webpack shared deps (avoids `system_toolchain.wasm` download failures during `yarn build`). Builds retry up to 5 times on transient GitHub 502s.

If bootstrap fails with `ENOTFOUND` / `EAI_AGAIN` on external hosts, fix Docker Desktop DNS (e.g. add `"dns": ["8.8.8.8"]` in Docker Engine settings) and rebuild:

```bash
docker build --no-cache -f kibana_plugin/Dockerfile.build -t cpm-kibana-plugin-build:8.19.16 kibana_plugin/
```

| File | Purpose |
|------|---------|
| `kibana_plugin/Dockerfile.build` | Clone Kibana, bootstrap, `yarn build`, copy zip to `/output` |
| `kibana_plugin/Dockerfile` | Same build + bake plugin into `kibana-cpm:<version>` image |

### Step 3 — Build manually (optional)

```bash
export KIBANA_VERSION=8.19.16   # or 9.4.3
export KIBANA_DIR=/tmp/kibana-${KIBANA_VERSION}
export PLUGIN_SRC="$(pwd)/kibana_plugin/cpm"   # from elastic_flex_cluster_with_cpm/

git clone --depth 1 --branch "v${KIBANA_VERSION}" \
  https://github.com/elastic/kibana.git "${KIBANA_DIR}"

cd "${KIBANA_DIR}"
corepack enable
yarn kbn bootstrap --skip-os-packages

rm -rf plugins/cpm
cp -a "${PLUGIN_SRC}" plugins/cpm

cd plugins/cpm
yarn build

ls build/cpm-${KIBANA_VERSION}.zip
```

For **9.4.3**: set `KIBANA_VERSION=9.4.3`, update manifests in Step 1, use a **fresh** `KIBANA_DIR`, fix any 8.x → 9.x API breaks.

### Step 4 — Install the zip

On a Kibana host (version must match the zip):

```bash
bin/kibana-plugin install file:///path/to/cpm-8.19.16.zip
# restart Kibana
```

Docker Compose:

```bash
docker compose cp kibana_plugin/build/cpm-8.19.16.zip kibana:/tmp/cpm.zip
docker compose exec kibana bin/kibana-plugin install file:///tmp/cpm.zip
docker compose restart kibana
```

Verify: **Stack Management → Ingest → Cluster Pipeline Manager**.

### Step 5 — Or bake into a Docker image (reference stack)

From `kibana_plugin/`:

```bash
# 8.19.16
docker build -t kibana-cpm:8.19.16 .

# 9.4.3
docker build --build-arg KIBANA_VERSION=9.4.3 -t kibana-cpm:9.4.3 .
```

Set `KIBANA_IMAGE=kibana-cpm:8.19.16` in `docker/reference/.env`, then `docker compose up -d kibana`.

### Version-bump checklist

1. `cpm/kibana.json` → `kibanaVersion`
2. `cpm/package.json` → `kibana.version`
3. Build with matching `KIBANA_VERSION` / git tag `v*`
4. New or cleared `KIBANA_DIR` for the new version
5. Resolve compile/API breaks (especially 8.x → 9.x)

### CI/CD sketch

```yaml
build-cpm-plugin:
  image: node:22-bookworm
  cache:
    key: kibana-v8.19.16
    paths: [.cache/kibana-8.19.16/]
  script:
    - apt-get update && apt-get install -y git python3 build-essential
    - export KIBANA_VERSION=8.19.16
    - export KIBANA_DIR=$CI_PROJECT_DIR/.cache/kibana-8.19.16
    - ./scripts/build_kibana_cpm_plugin.sh
  artifacts:
    paths: [kibana_plugin/build/cpm-*.zip]
```

---

## Permissions

### Nav visibility

Shown when the user has **any** cluster privilege: `monitor`, `manage`, `manage_pipeline`, `manage_logstash_pipelines`.

`kibana_admin` bypasses the Elasticsearch-feature check, so the plugin calls `GET /api/cpm/access` on startup and hides the nav item when cluster privileges are missing.

Not controlled by Space feature toggles.

### Using the UI

Same cluster privilege check on all `/api/cpm/*` routes, plus:

- Read/write on `cpm-cluster-registry`, `cpm-routing-config`
- `manage_watcher` for the **Run CPM** tab

---

## Local development

Symlink or copy `kibana_plugin/cpm` into `<kibana>/plugins/cpm` in a matching Kibana checkout, then:

```bash
cd <kibana>/plugins/cpm && yarn dev
cd <kibana> && yarn start
```

See [Elastic Kibana plugin docs](https://www.elastic.co/docs/extend/kibana).

**Note:** `cpm/tsconfig.json` extends Kibana's `tsconfig.base.json` — IDE errors in this repo alone are normal until a Kibana checkout exists (build still works via Step 2).

---

## Layout

```
kibana_plugin/
  cpm/                 # plugin source
    kibana.json        # manifest (id, kibanaVersion)
    package.json       # yarn build via Kibana plugin_helpers
    public/            # React UI
    server/routes/     # /api/cpm/*
    common/
  build/               # output zips (gitignored)
  Dockerfile           # optional image with plugin pre-installed
  Dockerfile.build     # zip-only builder (mount build/ as /output)
```

Build scripts: `scripts/build_kibana_cpm_plugin.sh` (native), `scripts/build_kibana_cpm_plugin_docker.sh` (Docker artifact).

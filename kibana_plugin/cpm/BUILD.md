# Build CPM plugin

Minimal guide to produce `cpm-<kibanaVersion>.zip`.

The plugin **must** be compiled against the exact Kibana version you run in production.

---

## Prerequisites

| Tool | Notes |
|------|--------|
| Git | shallow clone of `elastic/kibana` |
| Node.js **22.x** | e.g. 22.22.2 |
| Corepack + Yarn | `corepack enable` (Yarn version comes from Kibana after bootstrap) |
| `python3`, C build tools | needed by Kibana bootstrap (`build-essential` on Debian/Ubuntu) |
| ~8 GB disk | first clone + bootstrap |
| ~8 GB RAM | recommended for bootstrap |

---

## 1. Pick the Kibana version

Examples:

| Target Kibana | Git tag | Zip output |
|---------------|---------|------------|
| **8.19.16** | `v8.19.16` | `cpm-8.19.16.zip` |
| **9.4.3** | `v9.4.3` | `cpm-9.4.3.zip` |

Update the plugin manifest to match (only when switching versions):

```bash
# In this directory (kibana_plugin/cpm/)
# kibana.json  → "kibanaVersion": "8.19.16"   # or "9.4.3"
# package.json → "kibana": { "version": "8.19.16" }   # same value
```

---

## 2. Build (recommended — use the repo script)

From `Jan/elastic_flex_cluster_with_cpm/`:

```bash
# Kibana 8.19.16 (default)
./scripts/build_kibana_cpm_plugin.sh

# Kibana 9.4.3
KIBANA_VERSION=9.4.3 ./scripts/build_kibana_cpm_plugin.sh
```

Artifact: `kibana_plugin/build/cpm-<version>.zip`

The script clones Kibana into `.kibana-build/`, bootstraps once, copies `kibana_plugin/cpm` into `plugins/cpm`, runs `yarn build`, and copies the zip out.

**Cache between builds:** set `KIBANA_DIR` to a persistent path so bootstrap runs only once per version:

```bash
KIBANA_VERSION=9.4.3 \
KIBANA_DIR=$HOME/.cache/kibana-9.4.3 \
./scripts/build_kibana_cpm_plugin.sh
```

---

## 3. Build (manual steps)

Same flow as the script, if you prefer to run each step yourself.

### 8.19.16

```bash
export KIBANA_VERSION=8.19.16
export KIBANA_DIR=/tmp/kibana-${KIBANA_VERSION}
export PLUGIN_SRC=/path/to/DoD/Jan/elastic_flex_cluster_with_cpm/kibana_plugin/cpm

# 1) Clone Kibana (one-time per version)
git clone --depth 1 --branch "v${KIBANA_VERSION}" \
  https://github.com/elastic/kibana.git "${KIBANA_DIR}"

# 2) Bootstrap (one-time per checkout)
cd "${KIBANA_DIR}"
corepack enable
yarn kbn bootstrap --skip-os-packages

# 3) Install plugin source
rm -rf plugins/cpm
cp -a "${PLUGIN_SRC}" plugins/cpm

# 4) Build zip
cd plugins/cpm
yarn build

# 5) Collect artifact
ls build/cpm-${KIBANA_VERSION}.zip
```

### 9.4.3

Same commands; only change the version and manifest fields:

```bash
export KIBANA_VERSION=9.4.3
# ensure kibana.json + package.json kibana version = 9.4.3
# then run steps 1–5 above
```

---

## 4. Install the zip

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

---

## 5. Optional — Docker image with plugin baked in

From `kibana_plugin/` (parent of `cpm/`):

```bash
# 8.19.16
docker build -t kibana-cpm:8.19.16 .

# 9.4.3
docker build --build-arg KIBANA_VERSION=9.4.3 -t kibana-cpm:9.4.3 .
```

Point your stack at the image (e.g. `KIBANA_IMAGE=kibana-cpm:8.19.16`).

---

## Checklist when changing Kibana version

1. Set `kibana.json` → `kibanaVersion`
2. Set `package.json` → `kibana.version`
3. Build with matching `KIBANA_VERSION` / `v*` git tag
4. Use a **fresh** Kibana checkout (or new `KIBANA_DIR`) for the new version
5. Fix any compile/API breaks between 8.x and 9.x before the build succeeds

---

## Output layout

```
kibana_plugin/
  cpm/              ← source (this directory)
  build/
    cpm-8.19.16.zip ← script output (gitignored)
    cpm-9.4.3.zip
```

#!/usr/bin/env bash
# Build the CPM Kibana plugin zip for the installed Kibana version.
# Requires: git, node 22+, yarn (via corepack), ~8GB disk for kibana checkout.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLUGIN_SRC="${ROOT}/kibana_plugin/cpm"
KIBANA_VERSION="${KIBANA_VERSION:-8.19.16}"
KIBANA_DIR="${KIBANA_DIR:-${ROOT}/.kibana-build}"
OUT_DIR="${ROOT}/kibana_plugin/build"

echo "=== CPM Kibana plugin build (Kibana ${KIBANA_VERSION}) ==="

if [[ ! -d "${KIBANA_DIR}/.git" ]]; then
  echo "Cloning Kibana v${KIBANA_VERSION} into ${KIBANA_DIR} (one-time, ~5 min)..."
  git clone --depth 1 --branch "v${KIBANA_VERSION}" https://github.com/elastic/kibana.git "${KIBANA_DIR}"
fi

echo "Bootstrapping Kibana (one-time, may take several minutes)..."
cd "${KIBANA_DIR}"
if [[ ! -d node_modules ]]; then
  corepack enable 2>/dev/null || true
  yarn kbn bootstrap --skip-os-packages
fi

echo "Syncing plugin source..."
rm -rf "${KIBANA_DIR}/plugins/cpm"
cp -a "${PLUGIN_SRC}" "${KIBANA_DIR}/plugins/cpm"

echo "Building plugin..."
cd "${KIBANA_DIR}/plugins/cpm"
yarn build

mkdir -p "${OUT_DIR}"
rm -f "${OUT_DIR}"/cpm-*.zip
cp "${KIBANA_DIR}"/plugins/cpm/build/cpm-*.zip "${OUT_DIR}/"

ZIP="$(ls "${OUT_DIR}"/cpm-*.zip)"
echo ""
echo "Built: ${ZIP}"
echo ""
echo "Install on Kibana:"
echo "  docker compose exec kibana bin/kibana-plugin install file://${ZIP}"
echo "  # or mount unzip into /usr/share/kibana/plugins/cpm"

#!/usr/bin/env bash
# Build CPM Kibana plugin zip in Docker; write artifact to kibana_plugin/build/.
# Requires: Docker, network on first build (~15–20 min).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLUGIN_DIR="${ROOT}/kibana_plugin"
OUT_DIR="${PLUGIN_DIR}/build"
KIBANA_VERSION="${KIBANA_VERSION:-8.19.16}"
IMAGE="cpm-kibana-plugin-build:${KIBANA_VERSION}"

echo "=== CPM Kibana plugin Docker build (Kibana ${KIBANA_VERSION}) ==="

mkdir -p "${OUT_DIR}"

docker build \
  -f "${PLUGIN_DIR}/Dockerfile.build" \
  --build-arg "KIBANA_VERSION=${KIBANA_VERSION}" \
  -t "${IMAGE}" \
  "${PLUGIN_DIR}"

docker run --rm \
  -v "${OUT_DIR}:/output" \
  "${IMAGE}"

ZIP="${OUT_DIR}/cpm-${KIBANA_VERSION}.zip"
echo ""
echo "Artifact: ${ZIP}"
echo ""
echo "Install on Kibana:"
echo "  bin/kibana-plugin install file://${ZIP}"

#!/usr/bin/env bash
# Build the CPM Kibana plugin zip for Kibana 9.4.2 in Docker.
#
# 8.x -> 9.x differs in two ways that this script handles automatically:
#   1. Plugin manifests must declare kibanaVersion 9.4.2 (they ship on 8.19.16).
#   2. Kibana 9.4.2 pins Node 24.14.1; the base image is bumped from node:22.22.2.
#
# The manifest edits are written into kibana_plugin/cpm/ in place. The Node bump
# is applied to a throwaway Dockerfile so the shared Dockerfile.build (still used
# by the 8.x v8 script) is left untouched.
#
# Requires: Docker, network on first build (~15-20 min).
#
# Usage:
#   ./scripts/build_kibana_cpm_plugin_docker_v9.sh
#   KIBANA_URL=https://my-kibana:5601 ./scripts/build_kibana_cpm_plugin_docker_v9.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLUGIN_DIR="${ROOT}/kibana_plugin"
PLUGIN_SRC="${PLUGIN_DIR}/cpm"
OUT_DIR="${PLUGIN_DIR}/build"
KIBANA_VERSION="${KIBANA_VERSION:-9.4.2}"
NODE_VERSION="${NODE_VERSION:-24.14.1}"
IMAGE="cpm-kibana-plugin-build:${KIBANA_VERSION}"

echo "=== CPM Kibana plugin Docker build (Kibana ${KIBANA_VERSION}, Node ${NODE_VERSION}) ==="

# --- Step 1: Confirm target version (optional, only if KIBANA_URL is set) ------
if [[ -n "${KIBANA_URL:-}" ]]; then
  echo "Checking live Kibana version at ${KIBANA_URL} ..."
  LIVE_VERSION="$(curl -sk "${KIBANA_URL%/}/api/status" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['version']['number'])" 2>/dev/null || true)"
  echo "  reported: ${LIVE_VERSION:-<unreachable>}"
  if [[ -n "${LIVE_VERSION}" && "${LIVE_VERSION}" != "${KIBANA_VERSION}" ]]; then
    echo "ERROR: live Kibana is ${LIVE_VERSION} but building for ${KIBANA_VERSION}." >&2
    echo "       Set KIBANA_VERSION=${LIVE_VERSION} or point KIBANA_URL at the right host." >&2
    exit 1
  fi
fi

# --- Step 2: Update plugin manifests to the target version --------------------
echo "Pinning plugin manifests to Kibana ${KIBANA_VERSION} ..."
KIBANA_VERSION="${KIBANA_VERSION}" python3 - "${PLUGIN_SRC}" <<'PY'
import json, os, sys

plugin_src = sys.argv[1]
target = os.environ["KIBANA_VERSION"]

kibana_json = os.path.join(plugin_src, "kibana.json")
with open(kibana_json) as f:
    data = json.load(f)
data["kibanaVersion"] = target
with open(kibana_json, "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")

package_json = os.path.join(plugin_src, "package.json")
with open(package_json) as f:
    data = json.load(f)
data.setdefault("kibana", {})["version"] = target
with open(package_json, "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")

print(f"  kibana.json  -> kibanaVersion {target}")
print(f"  package.json -> kibana.version {target}")
PY

# --- Step 3/4b: Build in Docker with the 9.x Node version ---------------------
# Derive a temporary Dockerfile from Dockerfile.build with the Node base image
# swapped to the version Kibana 9.4.2 pins. The original is left unchanged.
TMP_DOCKERFILE="${PLUGIN_DIR}/Dockerfile.build.v9"
# Never let cleanup failure change the script's exit status. On macOS the
# generated file carries a com.apple.provenance xattr that a sandboxed shell may
# be denied permission to unlink; a leftover temp file must not fail the build.
cleanup() { rm -f "${TMP_DOCKERFILE}" 2>/dev/null || true; }
trap cleanup EXIT

sed -E "s|^FROM node:[0-9.]+-bookworm-slim|FROM node:${NODE_VERSION}-bookworm-slim|" \
  "${PLUGIN_DIR}/Dockerfile.build" > "${TMP_DOCKERFILE}"

if ! grep -q "FROM node:${NODE_VERSION}-bookworm-slim" "${TMP_DOCKERFILE}"; then
  echo "ERROR: could not rewrite the Node base image in Dockerfile.build." >&2
  echo "       Expected a 'FROM node:<ver>-bookworm-slim' line." >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"

docker build \
  -f "${TMP_DOCKERFILE}" \
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
echo ""
echo "Step 5 — If the build failed on TypeScript/plugin-API errors, the 8.x -> 9.x"
echo "         plugin APIs (management, features, types) likely changed. Read the"
echo "         errors above, fix kibana_plugin/cpm/ (public/plugin.ts, server/,"
echo "         imports), then re-run this script."

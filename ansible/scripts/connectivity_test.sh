#!/usr/bin/env bash
# Pre-flight API connectivity for CPM Ansible (kaposi.net inventory).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${CPM_ENV_FILE:-}"

if [[ -z "${ENV_FILE}" ]]; then
  for candidate in \
    "${ROOT}/.env" \
    "${ROOT}/../../../docker/reference/.env" \
    "${HOME}/DoD/docker/reference/.env"; do
    if [[ -f "${candidate}" ]]; then
      ENV_FILE="${candidate}"
      break
    fi
  done
fi

if [[ -n "${ENV_FILE}" && -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${ENV_FILE}"
  set +a
elif [[ -z "${ELASTIC_PASSWORD:-}" ]]; then
  echo "Could not load credentials."
  echo "  Tried: ${ROOT}/.env"
  echo "  Tried: ${ROOT}/../../../docker/reference/.env"
  echo ""
  echo "Fix one of:"
  echo "  ./scripts/setup_local.sh"
  echo "  scp imr-dod-vm:~/DoD/docker/reference/.env ${ROOT}/.env"
  echo "  export CPM_ENV_FILE=/path/to/.env"
  echo "  export ELASTIC_PASSWORD=..."
  exit 1
fi

ES_URL="${ELASTIC_BASE_URL:-https://central.kaposi.net}"
KB_URL="${KIBANA_BASE_URL:-https://cpm.kaposi.net}"
MON_INDEX="${CPM_MONITORING_INDEX:-.monitoring-es-8-*}"

pass() { echo "  OK  $*"; }
fail() { echo "  FAIL $*"; exit 1; }

es_curl() {
  curl -sfS -u "elastic:${ELASTIC_PASSWORD}" "$@" || return 1
}

kb_curl() {
  curl -sfS -u "elastic:${ELASTIC_PASSWORD}" "$@" || return 1
}

echo "=== CPM connectivity test ==="
echo "ES:     ${ES_URL}"
echo "Kibana: ${KB_URL}"
echo ""

health="$(es_curl "${ES_URL}/_cluster/health")" || fail "Elasticsearch ${ES_URL}"
cluster="$(echo "${health}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['cluster_name'], d['status'], 'nodes='+str(d['number_of_nodes']))")"
pass "Elasticsearch cluster health: ${cluster}"

lic="$(es_curl "${ES_URL}/_license" | python3 -c "import sys,json; print(json.load(sys.stdin)['license']['type'])")" || fail "license API"
pass "License: ${lic}"

mon="$(es_curl -X POST "${ES_URL}/${MON_INDEX}/_search" -H 'Content-Type: application/json' -d '{"size":0,"query":{"range":{"@timestamp":{"gte":"now-1d"}}}}' | python3 -c "import sys,json; d=json.load(sys.stdin); t=d.get('hits',{}).get('total',{}); print(t.get('value',t) if isinstance(t,dict) else t)")" || fail "monitoring search"
pass "Monitoring docs (24h): ${mon}"

es_curl "${ES_URL}/_watcher/stats" >/dev/null || fail "Watcher API"
pass "Watcher API"

es_curl "${ES_URL}/_ml/info" >/dev/null || fail "ML API"
pass "ML API"

es_curl "${ES_URL}/_transform" >/dev/null || fail "Transform API"
pass "Transform API"

kb_status="$(curl -sfS "${KB_URL}/api/status" | python3 -c "import sys,json; print(json.load(sys.stdin)['status']['overall']['level'])")" || fail "Kibana /api/status"
pass "Kibana status: ${kb_status}"

kb_curl "${KB_URL}/api/spaces/space" >/dev/null || fail "Kibana spaces API"
pass "Kibana spaces API (authenticated)"

kb_curl "${KB_URL}/api/saved_objects/_find?type=index-pattern&per_page=1" >/dev/null || fail "Kibana saved objects API"
pass "Kibana saved objects API (authenticated)"

remote="$(es_curl "https://cluster05.kaposi.net/_cluster/health" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")" || fail "remote cluster05"
pass "Remote cluster05: ${remote}"

echo ""
echo "All connectivity checks passed."

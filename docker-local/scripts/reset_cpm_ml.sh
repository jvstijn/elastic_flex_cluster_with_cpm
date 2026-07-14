#!/usr/bin/env bash
# Reset CPM ML jobs and replay datafeeds from historical monitoring.
set -euo pipefail

ES="${ES_HOST:-https://localhost:9200}"
AUTH="${ES_USER:-elastic}:${ES_PASSWORD:-changeme-elastic}"
CURL=(curl -sk -u "$AUTH")
START="${ML_START:-now-7d}"

JOBS=(cpm-store-size cpm-jvm-heap cpm-shard-count cpm-cluster-event-rate cpm-event-rate)

echo "=== Stop datafeeds ==="
for job in "${JOBS[@]}"; do
  feed="datafeed-${job}"
  "${CURL[@]}" -X POST "$ES/_ml/datafeeds/${feed}/_stop?force=true" >/dev/null 2>&1 || true
done

echo "=== Close and reset ML jobs ==="
for job in "${JOBS[@]}"; do
  "${CURL[@]}" -X POST "$ES/_ml/anomaly_detectors/${job}/_close?force=true" >/dev/null 2>&1 || true
  resp=$("${CURL[@]}" -X POST "$ES/_ml/anomaly_detectors/${job}/_reset")
  echo "${job} reset: ${resp}"
  "${CURL[@]}" -X POST "$ES/_ml/anomaly_detectors/${job}/_open" >/dev/null
done

echo "=== Restart datafeeds from ${START} ==="
for job in "${JOBS[@]}"; do
  feed="datafeed-${job}"
  resp=$("${CURL[@]}" -X POST "$ES/_ml/datafeeds/${feed}/_start" \
    -H 'Content-Type: application/json' \
    -d "{\"start\":\"${START}\"}")
  echo "${feed}: ${resp}"
done

echo "=== Done. Poll with: GET _ml/anomaly_detectors/cpm-store-size/_stats ==="

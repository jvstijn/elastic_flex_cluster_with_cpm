#!/usr/bin/env bash
set -euo pipefail

CA="${CA_CERT:-/certs/ca/ca.crt}"
MARKER="/setup/state/.stack-ready"
STATE_DIR="/setup/state"

mkdir -p "${STATE_DIR}"

es_curl() {
  local host="$1"
  local path="$2"
  shift 2
  curl -sS --cacert "${CA}" -u "elastic:${ELASTIC_PASSWORD}" "$@" "https://${host}:9200${path}"
}

wait_for_es() {
  local host="$1"
  echo "Waiting for ${host}..."
  until curl -sS --cacert "${CA}" "https://${host}:9200" 2>&1 | grep -q 'missing authentication credentials'; do
    sleep 5
  done
}

start_trial() {
  local host="$1"
  echo "Starting trial license on ${host}..."
  local response
  response="$(es_curl "${host}" "/_license/start_trial?acknowledge=true" -X POST || true)"
  if echo "${response}" | grep -q '"trial_was_started":true'; then
    echo "Trial started on ${host}"
  elif echo "${response}" | grep -q 'Operation failed'; then
    echo "Trial already active or unavailable on ${host}: ${response}"
  else
    echo "Trial response from ${host}: ${response}"
  fi
}

set_builtin_password() {
  local host="$1"
  local user="$2"
  local password="$3"
  echo "Setting ${user} password on ${host}..."
  until es_curl "${host}" "/_security/user/${user}/_password" -X POST \
    -H "Content-Type: application/json" \
    -d "{\"password\":\"${password}\"}" | grep -q '^{}'; do
    sleep 5
  done
}

create_monitoring_users() {
  echo "Configuring monitoring users..."

  for host in es-central es-remote-a es-remote-b; do
    set_builtin_password "${host}" remote_monitoring_user "${MONITORING_PASSWORD}"
  done

  es_curl es-central "/_security/user/remote_monitoring_agent" -X PUT \
    -H "Content-Type: application/json" \
    -d "{
      \"password\": \"${MONITORING_PASSWORD}\",
      \"roles\": [\"remote_monitoring_agent\"],
      \"full_name\": \"Remote Monitoring Agent\"
    }" >/dev/null || true
}

create_ccs_api_key() {
  local host="$1"
  local name="$2"
  es_curl "${host}" "/_security/cross_cluster/api_key" -X POST \
    -H "Content-Type: application/json" \
    -d "{
      \"name\": \"${name}\",
      \"access\": {
        \"search\": [
          {
            \"names\": [\"*\"],
            \"allow_restricted_indices\": false
          }
        ]
      }
    }"
}

add_keystore_credential() {
  local alias="$1"
  local encoded="$2"
  echo "Adding cross-cluster API key for ${alias} to es-central keystore"
  docker compose exec -T es-central \
    bash -c "echo '${encoded}' | bin/elasticsearch-keystore add --stdin --force cluster.remote.${alias}.credentials"
}

reload_secure_settings() {
  echo "Reloading secure settings on es-central..."
  es_curl es-central "/_nodes/reload_secure_settings" -X POST \
    -H "Content-Type: application/json" \
    -d '{}'
}

configure_remote_clusters() {
  echo "Configuring remote cluster connections on es-central..."
  es_curl es-central "/_cluster/settings" -X PUT \
    -H "Content-Type: application/json" \
    -d '{
      "persistent": {
        "cluster.remote.remote_a.seeds": ["es-remote-a:9443"],
        "cluster.remote.remote_b.seeds": ["es-remote-b:9443"],
        "cluster.remote.monitoring.mode": "sniff",
        "cluster.remote.monitoring.seeds": ["es-central:9300"],
        "cluster.remote.monitoring.skip_unavailable": true
      }
    }'
}

create_ccs_role() {
  echo "Creating CCS role for elastic user convenience..."
  es_curl es-central "/_security/role/ccs_admin" -X PUT \
    -H "Content-Type: application/json" \
    -d '{
      "cluster": ["monitor"],
      "indices": [
        {
          "names": ["*"],
          "privileges": ["read", "view_index_metadata", "monitor"]
        }
      ],
      "remote_indices": [
        {
          "clusters": ["remote_a", "remote_b"],
          "names": ["*"],
          "privileges": ["read", "view_index_metadata", "monitor"]
        }
      ]
    }' >/dev/null || true

  es_curl es-central "/_security/user/elastic" -X PUT \
    -H "Content-Type: application/json" \
    -d '{
      "roles": ["superuser", "ccs_admin"],
      "full_name": "Elastic Superuser",
      "email": "elastic@example.com"
    }' >/dev/null || true
}

seed_logstash_pipeline() {
  echo "Seeding bootstrap Logstash pipeline on es-central..."
  local modified
  modified="$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")"
  es_curl es-central "/_logstash/pipeline/kafka-to-central" -X PUT \
    -H "Content-Type: application/json" \
    -d "{
      \"pipeline\": \"input {\\n  kafka {\\n    bootstrap_servers => \\\"kafka:9092\\\"\\n    topics => [\\\"logs-beats-raw\\\"]\\n    group_id => \\\"logstash-managed\\\"\\n    codec => json\\n    decorate_events => true\\n  }\\n}\\n\\noutput {\\n  elasticsearch {\\n    hosts => [\\\"https://es-central:9200\\\"]\\n    user => \\\"elastic\\\"\\n    password => \\\"${ELASTIC_PASSWORD}\\\"\\n    ssl_certificate_verification => true\\n    cacert => \\\"/usr/share/logstash/config/certs/ca/ca.crt\\\"\\n    data_stream => true\\n    data_stream_type => \\\"logs\\\"\\n    data_stream_dataset => \\\"generic\\\"\\n    data_stream_namespace => \\\"default\\\"\\n  }\\n}\\n\",
      \"pipeline_settings\": {
        \"pipeline.workers\": 2,
        \"queue.type\": \"persisted\"
      },
      \"pipeline_metadata\": {},
      \"username\": \"elastic\",
      \"last_modified\": \"${modified}\"
    }"
}

verify_remote_clusters() {
  echo "Verifying remote cluster connections..."
  es_curl es-central "/_remote/info?pretty"
}

if [ -f "${MARKER}" ]; then
  echo "Stack already initialized (${MARKER} exists). Skipping."
  exit 0
fi

wait_for_es es-central
wait_for_es es-remote-a
wait_for_es es-remote-b

set_builtin_password es-central kibana_system "${KIBANA_PASSWORD}"
set_builtin_password es-central logstash_system "${LOGSTASH_PASSWORD}"

for host in es-central es-remote-a es-remote-b; do
  start_trial "${host}"
done

create_monitoring_users

remote_a_key="$(create_ccs_api_key es-remote-a ccs-from-central-remote-a)"
remote_b_key="$(create_ccs_api_key es-remote-b ccs-from-central-remote-b)"

remote_a_encoded="$(echo "${remote_a_key}" | grep -o '"encoded":"[^"]*"' | head -1 | cut -d'"' -f4)"
remote_b_encoded="$(echo "${remote_b_key}" | grep -o '"encoded":"[^"]*"' | head -1 | cut -d'"' -f4)"

add_keystore_credential remote_a "${remote_a_encoded}"
add_keystore_credential remote_b "${remote_b_encoded}"

reload_secure_settings
configure_remote_clusters
create_ccs_role
seed_logstash_pipeline
verify_remote_clusters

touch "${MARKER}"
echo "Stack initialization complete."

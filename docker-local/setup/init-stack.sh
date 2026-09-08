#!/usr/bin/env bash
set -euo pipefail

CA="${CA_CERT:-/certs/ca/ca.crt}"
MARKER="/setup/state/.stack-ready"
STATE_DIR="/setup/state"
ENV_FILE="/workspace/.env"

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

  for host in es-central es-remote-a es-remote-b es-monitoring; do
    set_builtin_password "${host}" remote_monitoring_user "${MONITORING_PASSWORD}"
  done

  # De agent-user is de OUTPUT-user van Metricbeat en moet dus op het
  # monitoring-cluster staan, niet op es-central.
  es_curl es-monitoring "/_security/user/remote_monitoring_agent" -X PUT \
    -H "Content-Type: application/json" \
    -d "{
      \"password\": \"${MONITORING_PASSWORD}\",
      \"roles\": [\"remote_monitoring_agent\"],
      \"full_name\": \"Remote Monitoring Agent\"
    }" >/dev/null || true
}

# Centralized pipeline management: Logstash mag alleen pipelines lezen, en heeft
# cluster:monitor nodig voor de license-check op /. Zonder deze user stopt elke
# CPM-Logstash met "unable to authenticate user [cpm_logstash]" -> FATAL.
create_cpm_logstash_user() {
  echo "Creating cpm_logstash role and user on es-central..."
  es_curl es-central "/_security/role/cpm_logstash_pipelines" -X PUT \
    -H "Content-Type: application/json" \
    -d '{
      "cluster": ["monitor", "manage_logstash_pipelines"]
    }' >/dev/null

  es_curl es-central "/_security/user/cpm_logstash" -X PUT \
    -H "Content-Type: application/json" \
    -d "{
      \"password\": \"${LOGSTASH_PASSWORD}\",
      \"roles\": [\"cpm_logstash_pipelines\"],
      \"full_name\": \"CPM centralized pipeline management\"
    }" >/dev/null
}

# Ingest-key per cluster: hiermee schrijft de door CPM gerenderde pipeline zijn
# data streams weg. De pipeline-templates gebruiken hem als ${VAR_API_KEY}.
create_ingest_api_key() {
  local host="$1"
  local name="$2"
  es_curl "${host}" "/_security/api_key" -X POST \
    -H "Content-Type: application/json" \
    -d "{
      \"name\": \"${name}\",
      \"role_descriptors\": {
        \"cpm_ingest\": {
          \"cluster\": [\"monitor\", \"manage_index_templates\"],
          \"indices\": [
            {
              \"names\": [\"logs-*\", \"metrics-*\", \"traces-*\", \"filebeat-*\"],
              \"privileges\": [\"auto_configure\", \"create_doc\", \"create\", \"write\", \"view_index_metadata\"]
            }
          ]
        }
      }
    }"
}

encoded_of() {
  echo "$1" | grep -o '"encoded":"[^"]*"' | head -1 | cut -d'"' -f4
}

# Zet KEY=waarde in .env: bestaande regel vervangen, anders toevoegen.
put_env_var() {
  local key="$1"
  local value="$2"
  if [ ! -w "${ENV_FILE}" ]; then
    echo "WARN: ${ENV_FILE} is niet schrijfbaar; zet ${key} handmatig."
    return 0
  fi
  if grep -q "^${key}=" "${ENV_FILE}"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "${ENV_FILE}"
  else
    printf '%s=%s\n' "${key}" "${value}" >> "${ENV_FILE}"
  fi
  echo "  ${key} bijgewerkt in .env"
}

create_ingest_keys() {
  echo "Creating per-cluster ingest API keys..."
  local central_key remote_a_key remote_b_key
  central_key="$(encoded_of "$(create_ingest_api_key es-central cpm-ingest-central)")"
  remote_a_key="$(encoded_of "$(create_ingest_api_key es-remote-a cpm-ingest-remote-a)")"
  remote_b_key="$(encoded_of "$(create_ingest_api_key es-remote-b cpm-ingest-remote-b)")"

  for pair in "VAR_API_KEY_CENTRAL:${central_key}" \
              "VAR_API_KEY_REMOTE_A:${remote_a_key}" \
              "VAR_API_KEY_REMOTE_B:${remote_b_key}"; do
    local k="${pair%%:*}" v="${pair#*:}"
    if [ -z "${v}" ]; then
      echo "WARN: geen key gekregen voor ${k}"
      continue
    fi
    printf '%s=%s\n' "${k}" "${v}" >> "${STATE_DIR}/api-keys.env"
    put_env_var "${k}" "${v}"
  done

  echo "Herstart de logstash-services zodat ze de nieuwe VAR_API_KEY_* oppikken:"
  echo "  docker compose -f docker-local/docker-compose.yml up -d --force-recreate \\"
  echo "      logstash-central logstash-remote-a logstash-remote-b"
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
        "cluster.remote.monitoring.seeds": ["es-monitoring:9443"],
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
          "clusters": ["remote_a", "remote_b", "monitoring"],
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

# De keystore van es-central staat in de containerlaag, niet op een volume.
# Zodra de container opnieuw wordt aangemaakt (en dat gebeurt al als .env
# wijzigt) zijn de cross-cluster credentials weg, valt elke CCS-search stil en
# ziet CPM geen monitoring-data meer. Deze stap draait daarom bij ELKE start.
# De sleutels zelf worden hergebruikt uit ${STATE_DIR}/ccs-keys.env, zodat er
# niet bij iedere herstart een nieuwe API-key bijkomt.
ensure_ccs_credentials() {
  local keyfile="${STATE_DIR}/ccs-keys.env"
  # shellcheck disable=SC1090
  [ -f "${keyfile}" ] && . "${keyfile}"

  local changed=0
  if [ -z "${CCS_KEY_MONITORING:-}" ]; then
    echo "Creating cross-cluster API key on es-monitoring..."
    CCS_KEY_MONITORING="$(encoded_of "$(create_ccs_api_key es-monitoring ccs-from-central-monitoring)")"
    changed=1
  fi
  if [ -z "${CCS_KEY_REMOTE_A:-}" ]; then
    echo "Creating cross-cluster API key on es-remote-a..."
    CCS_KEY_REMOTE_A="$(encoded_of "$(create_ccs_api_key es-remote-a ccs-from-central-remote_a)")"
    changed=1
  fi
  if [ -z "${CCS_KEY_REMOTE_B:-}" ]; then
    echo "Creating cross-cluster API key on es-remote-b..."
    CCS_KEY_REMOTE_B="$(encoded_of "$(create_ccs_api_key es-remote-b ccs-from-central-remote_b)")"
    changed=1
  fi

  if [ "${changed}" -eq 1 ]; then
    : > "${keyfile}"
    printf 'CCS_KEY_MONITORING=%s\n' "${CCS_KEY_MONITORING}" >> "${keyfile}"
    printf 'CCS_KEY_REMOTE_A=%s\n' "${CCS_KEY_REMOTE_A}" >> "${keyfile}"
    printf 'CCS_KEY_REMOTE_B=%s\n' "${CCS_KEY_REMOTE_B}" >> "${keyfile}"

    echo "Cross-cluster sleutels opgeslagen in ${keyfile}"
  fi

  add_keystore_credential monitoring "${CCS_KEY_MONITORING}"
  add_keystore_credential remote_a "${CCS_KEY_REMOTE_A}"
  add_keystore_credential remote_b "${CCS_KEY_REMOTE_B}"

  reload_secure_settings
  configure_remote_clusters
}

for host in es-central es-remote-a es-remote-b es-monitoring; do
  wait_for_es "${host}"
done

# Trial eerst: cross-cluster API-keys vragen een licentie die verder gaat
# dan basic.
for host in es-central es-remote-a es-remote-b es-monitoring; do
  start_trial "${host}"
done

ensure_ccs_credentials

if [ -f "${MARKER}" ]; then
  echo "Stack already initialized (${MARKER} exists). Skipping."
  exit 0
fi


set_builtin_password es-central kibana_system "${KIBANA_PASSWORD}"
# Stack Monitoring in Kibana bevraagt het monitoring-cluster rechtstreeks.
set_builtin_password es-monitoring kibana_system "${KIBANA_PASSWORD}"
set_builtin_password es-central logstash_system "${LOGSTASH_PASSWORD}"


create_monitoring_users
create_cpm_logstash_user
create_ingest_keys

create_ccs_role
seed_logstash_pipeline
verify_remote_clusters

touch "${MARKER}"
echo "Stack initialization complete."

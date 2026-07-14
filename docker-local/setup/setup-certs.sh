#!/usr/bin/env bash
set -euo pipefail

if [ -z "${ELASTIC_PASSWORD:-}" ]; then
  echo "Set ELASTIC_PASSWORD in the .env file"
  exit 1
fi

if [ -z "${KIBANA_PASSWORD:-}" ]; then
  echo "Set KIBANA_PASSWORD in the .env file"
  exit 1
fi

if [ -z "${LOGSTASH_PASSWORD:-}" ]; then
  echo "Set LOGSTASH_PASSWORD in the .env file"
  exit 1
fi

CERTS_DIR="/usr/share/elasticsearch/config/certs"

mkdir -p "${CERTS_DIR}"

if [ ! -f "${CERTS_DIR}/ca.zip" ]; then
  echo "Creating CA"
  bin/elasticsearch-certutil ca --silent --pem -out "${CERTS_DIR}/ca.zip"
  unzip -o "${CERTS_DIR}/ca.zip" -d "${CERTS_DIR}"
fi

if [ ! -f "${CERTS_DIR}/certs.zip" ]; then
  echo "Creating node certificates"
  cat > "${CERTS_DIR}/instances.yml" <<'EOF'
instances:
  - name: es-central
    dns:
      - es-central
      - localhost
    ip:
      - 127.0.0.1
  - name: es-remote-a
    dns:
      - es-remote-a
      - localhost
    ip:
      - 127.0.0.1
  - name: es-remote-b
    dns:
      - es-remote-b
      - localhost
    ip:
      - 127.0.0.1
EOF
  bin/elasticsearch-certutil cert --silent --pem \
    -out "${CERTS_DIR}/certs.zip" \
    --in "${CERTS_DIR}/instances.yml" \
    --ca-cert "${CERTS_DIR}/ca/ca.crt" \
    --ca-key "${CERTS_DIR}/ca/ca.key"
  unzip -o "${CERTS_DIR}/certs.zip" -d "${CERTS_DIR}"
fi

echo "Setting certificate permissions"
chown -R root:root "${CERTS_DIR}"
find "${CERTS_DIR}" -type d -exec chmod 750 {} \;
find "${CERTS_DIR}" -type f -exec chmod 640 {} \;

echo "Certificate setup complete"

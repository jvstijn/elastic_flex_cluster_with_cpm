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

# Ook opnieuw genereren als er een node bijgekomen is; anders mist een
# bestaand certs-volume het certificaat van es-monitoring.
if [ ! -f "${CERTS_DIR}/certs.zip" ] || [ ! -d "${CERTS_DIR}/es-monitoring" ]; then
  echo "Creating node certificates"
  # certutil weigert te schrijven als het zip er al ligt; bij een uitgebreide
  # instances.yml moet het oude archief dus eerst weg. De al uitgepakte
  # certificaten blijven staan en worden hooguit overschreven.
  rm -f "${CERTS_DIR}/certs.zip"
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
  - name: es-monitoring
    dns:
      - es-monitoring
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

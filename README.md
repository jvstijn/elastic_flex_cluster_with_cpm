# Docker Elastic Stack (CPM demo)

Three Elasticsearch clusters, Kibana, Kafka, two Logstash instances, Metricbeat monitoring, and CCS — wired for CPM (Cluster Pipeline Manager).

## Services

| Service | Role |
|---------|------|
| `es-central` (:9200) | CCS hub, CPM control plane, Logstash pipeline API |
| `es-remote-a` (:9201) | Remote cluster A |
| `es-remote-b` (:9202) | Remote cluster B |
| `logstash-beats` (:5044) | Beats → Kafka (`logs-beats-raw` topic) |
| `logstash-managed` | Centrally managed pipelines from es-central (CPM) |
| `kafka` | Ingest bus between Logstash instances |

## Quick start

```bash
cd docker
cp .env.example .env          # adjust passwords if needed
docker compose up -d
```

Wait for `init` to complete (trial license, CCS, bootstrap pipeline).

## CPM setup

```bash
cd cpm
python3 -m venv .venv && source .venv/bin/activate
pip install requests
cp cpm_settings.json.example cpm_settings.json   # or copy docker/cpm_settings.docker.json.example
# Fill es_api_key (see cpm/README.md)
# docker-ca.crt in cpm/ is used automatically for https://localhost:9200

python3 cpm_install.py
python3 ../docker/scripts/backfill_monitoring_history.py   # optional: 7d ML history
python3 ../docker/scripts/bootstrap_cpm_pipelines.py         # templates + routing + pipelines
```

Or in one step after ML models are ready:

```bash
python3 cpm_install.py --bootstrap
```

## What bootstrap does

1. Seeds `cpm-pipeline-templates` (dedicated + catchall)
2. Runs `cpm-registry-sync`, then patches `ingest_hosts` / `dc` on each cluster
3. Runs scoring → routing-advisor → state-manager
4. Pushes CPM pipelines to `logstash-managed` via `/_logstash/pipeline`
5. Removes the bootstrap `kafka-to-central` pipeline

Expected pipelines on `logstash-managed`:

- **Central catchall** — Kafka topics `logs-beats-raw`, discovered datasets → `es-central`
- **Dedicated / catchall per remote cluster** — based on routing scores and monitoring

## Verify

```bash
curl -sk -u elastic:$ELASTIC_PASSWORD https://localhost:9200/_logstash/pipeline
curl -sk -u elastic:$ELASTIC_PASSWORD https://localhost:9200/cpm-pipeline-state/_search?size=20
curl -sk -u elastic:$ELASTIC_PASSWORD https://localhost:9200/cpm-scores/_search?sort=scored_at:desc&size=1
```

## Load simulation

```bash
python3 docker/scripts/simulate_auditd_load.py      # 1M docs → central
python3 docker/scripts/simulate_nginx_load.py       # nginx → remote-a
# Re-run bootstrap or state-manager + pipeline-manager after new datasets appear
```

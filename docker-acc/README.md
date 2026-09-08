# docker-acc — acceptatie-omgeving

Drie eigen Elasticsearch-clusters met een eigen CPM, naast de bestaande
`docker-local` stack. De scheiding tussen acceptatie en productie zit in de
**Kafka-topicnaam**: acc leest alleen topics die op `-acc` eindigen, productie
leest de kale namen. Er is dus één Kafka-cluster (die van `docker-local`) en
twee sets topics.

| | docker-local | docker-acc |
|---|---|---|
| clusters | es-central, es-remote-a, es-remote-b | es-acc-central, es-acc-remote-a, es-acc-remote-b |
| monitoring-cluster | es-monitoring (:9203) | es-acc-monitoring (:9213) |
| dc-attributen | dc-central, dc-a, dc-b | acc-central, acc-a, acc-b |
| ES-poorten | 9200, 9201, 9202 | 9210, 9211, 9212 |
| Kibana | 5601 | 5602 |
| Kafka | draait hier | gebruikt die van docker-local |
| topics | `logs-nginx-prod` | `logs-nginx-prod-acc` |
| inventory | `inventories/local` | `inventories/acc` |

De `-acc` komt uit `kafka_topic_extension` in `ansible/group_vars/acc.yml`.
Ansible zet die waarde in de `cpm-pipeline-templates` documenten; de
`cpm-pipeline-manager` watcher plakt hem achter `__TOPIC__` en achter elk topic
in `__TOPICS_LIST__`. Productie (`group_vars/prd.yml`) heeft `""` en verandert
dus niet.

## Opstarten

**1. Kafka moet draaien** — de acc-stack heeft geen eigen brokers:

```bash
docker compose -f docker-local/docker-compose.yml up -d
docker network ls | grep dod-elastic     # controleer de netwerknaam
```

Wijkt die af van `dod-elastic_dod-elastic`? Pas dan `KAFKA_NETWORK` in `.env` aan.

**2. `.env` invullen** (is al aangemaakt vanuit `.env.example`):

```bash
$EDITOR docker-acc/.env       # de vier wachtwoorden
```

Houd ze gelijk aan `docker-local/.env`; dat scheelt verwarring.

**3. Stack starten:**

```bash
docker compose -f docker-acc/docker-compose.yml up -d
docker compose -f docker-acc/docker-compose.yml logs -f init
```

`init` doet de eenmalige inrichting: trial-licenties, monitoring-users, de
`cpm_logstash`-user voor centralized pipeline management, en een ingest-API-key
per cluster. Die keys schrijft hij terug in `.env` als `VAR_API_KEY_ACC_*` —
daarna moeten de Logstash-containers ze nog oppikken:

```bash
docker compose -f docker-acc/docker-compose.yml up -d --force-recreate \
    logstash-acc-central logstash-acc-remote-a logstash-acc-remote-b
```

De Kibana van deze stack wordt lokaal gebouwd uit
`kibana_plugin/Dockerfile.install`, zodat de CPM-plugin er altijd in zit. Bouwt
compose hem niet (bijvoorbeeld na het vervangen van de zip), forceer dan:

```bash
docker compose -f docker-acc/docker-compose.yml up -d --build kibana
docker exec dod-elastic-acc-kibana-1 ls plugins    # -> cpm
```

**4. CPM installeren:**

```bash
cd ansible
.venv/bin/ansible-playbook -i inventories/acc/hosts site.yml
```

De inventory zet de host in de groep `acc`, waardoor `group_vars/acc.yml` geldt
en `kafka_topic_extension` op `-acc` staat. Controleer na afloop:

```bash
curl -sk -u elastic:$PW https://localhost:9210/cpm-pipeline-templates/_doc/catchall \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["_source"]["kafka_topic_extension"])'
# -> -acc
```

## De acc-topics aanmaken

Voor elk topic dat nu in Kafka staat een `-acc` tweelingtopic:

```bash
python3 scripts/create_kafka_topics.py --source kafka --suffix=-acc --dry-run
python3 scripts/create_kafka_topics.py --source kafka --suffix=-acc
```

Idempotent: topics die al op `-acc` eindigen worden overgeslagen, dus je krijgt
nooit `...-acc-acc`. Draai het opnieuw als er in productie topics bijkomen.

## Data door de keten

```bash
# 1. events in de -acc topics (laat de productie-topics met rust)
python3 scripts/fill_kafka_events.py --only-suffix=-acc --dry-run
python3 scripts/fill_kafka_events.py --only-suffix=-acc

# 2. de CPM-cyclus draaien op es-acc-central
python3 scripts/cpm_run_now.py --host https://localhost:9210 --insecure
```

Daarna bouwt de pipeline-manager de Logstash-pipelines met `-acc` topics, halen
de drie `logstash-acc-*` containers hun eigen pipelines op (gefilterd op hun
dc-prefix) en lopen de events naar de acc-clusters. Controleren:

```bash
# pipelines met acc-topics
curl -sk -u elastic:$PW https://localhost:9210/_logstash/pipeline | grep -o '\-acc"' | head

# data aangekomen
for p in 9210 9211 9212; do
  echo "== $p"; curl -sk -u elastic:$PW "https://localhost:$p/_cat/indices/logs-*?v&h=index,docs.count"
done
```

## Stoppen en opruimen

```bash
docker compose -f docker-acc/docker-compose.yml down          # containers weg, data blijft
docker compose -f docker-acc/docker-compose.yml down -v       # ook volumes + init-marker
```

Na `down -v` maakt `init` bij de volgende start nieuwe API-keys aan en werkt
`.env` opnieuw bij.

# Kafka-cluster + per-cluster Logstash + Kafka GUI

Uitbreiding van de stack met een klein Kafka-cluster, een web-GUI en één
CPM-managed Logstash per Elasticsearch-cluster. Datum: 2026-07-01 · branch `mod-jan`.

## Wat is toegevoegd

| Service | Omschrijving |
|---|---|
| `kafka`, `kafka2`, `kafka3` | 3-broker KRaft-cluster (combined broker+controller). Bootstrap: `kafka:9092,kafka2:9092,kafka3:9092`. RF 3, `min.insync.replicas=2`. |
| `kafka-ui` | Web-GUI (provectuslabs/kafka-ui) → **http://localhost:${KAFKA_UI_PORT}** (default 8080). Toont topics, partities, consumer-groups en messages. |
| `logstash-central`, `logstash-remote-a`, `logstash-remote-b` | Eén CPM-managed Logstash per ES-cluster; elk draait via central pipeline management alleen de pipelines van zijn eigen cluster. Vervangt de oude `logstash-managed`. |

Verder gewijzigd:
- `node.attr.dc=central|remote-a|remote-b` op de drie ES-nodes. `register-sync`
  schrijft deze `dc` naar `cpm-cluster-registry`, waardoor de CPM-pipeline-ids
  `<dc>_cpm-catchall-<uuid>` worden. Elke Logstash filtert daarop via
  `xpack.management.pipeline.id: ["<dc>_*"]`.
- `kafka_bootstrap` in de 4 pipeline-templates → `kafka:9092,kafka2:9092,kafka3:9092`.
- Nieuwe env-vars in `.env` (`.env.example`): `KAFKA_UI_PORT`, `KAFKA_CLUSTER_ID`
  (gedeelde KRaft cluster-id voor de 3 brokers).
- Kafka-volumes: `kafka1-data`, `kafka2-data`, `kafka3-data` (de oude `kafka-data`
  vervalt; single→3-broker vereist verse volumes met de gedeelde cluster-id).

## Opstartvolgorde, persistentie & geheugen

- **Opstartvolgorde (healthchecks).** De 3 brokers hebben een TCP-healthcheck
  (`bash /dev/tcp/localhost/9092`); `kafka-ui` en alle Logstash-services hangen
  aan `kafka`/`kafka2`/`kafka3` met `condition: service_healthy` (niet
  `service_started`). Zo starten de clients pas als het cluster echt bereikbaar
  is — geen "connection refused" meer in het ~30s quorum-venster.
- **Persistentie.** `KAFKA_LOG_DIRS: /var/lib/kafka/data` op alle 3 brokers.
  Zonder deze setting schrijft de apache/kafka-image naar het ephemeral
  `/tmp/kafka-logs` en gaat álle topic-data verloren bij een container-recreate.
  Met deze setting landt de data op de `kafka{1,2,3}-data`-volumes en blijft
  behouden.
- **Geheugen.** Footprint ~14,6 GB (es-central 2 GB, es-remote-a/b 1 GB, kibana
  1 GB, 3× kafka 768 MB, kafka-ui 512 MB, 5× logstash 1 GB, 3× metricbeat
  300 MB). Kafka-heaps 384 MB, Logstash-heaps 256 MB. Pas aan bij een kleinere
  Docker-VM.
- **logstash-beats.** Gebruikt `api.http.host` (in 9.4 is `http.host` verwijderd;
  de oude waarde liet de container crashen).

## In gebruik nemen

```bash
# 1. Kafka-cluster + GUI
docker compose up -d kafka kafka2 kafka3 kafka-ui

# 2. Topics aanmaken (leest de topics uit de CPM Logstash-pipelines)
python3 scripts/create_kafka_topics.py --insecure          # of --password ...

# 3. ES-nodes met de dc-attribuut + per-cluster Logstash activeren
#    (recreate van de ES-nodes; data-volumes blijven behouden)
docker compose up -d --remove-orphans

# 4. CPM de pipelines laten hernoemen naar <dc>_cpm-* (of wacht op de dagelijkse cron)
#    - metricbeat verzamelt de node.attr.dc in cluster-stats
#    - dan register-sync -> state-manager -> pipeline-manager uitvoeren
```

Na stap 4 hebben de pipelines het `dc`-prefix en pikt elke Logstash automatisch
zijn eigen cluster-pipelines op.

## Kafka GUI

Open **http://localhost:8080** (of `KAFKA_UI_PORT`). Cluster `dod-kafka` toont de
289 aangemaakte topics (`<type>-<dataset>-<namespace>`, plus `filebeat` en
`logs-beats-raw`), elk met replicatiefactor 3 over de drie brokers.

## Status van deze opzet (geverifieerd)

- ✅ 3-broker KRaft-cluster gevormd (3 voters, leader id 1).
- ✅ kafka-ui bereikbaar (HTTP 200 op :8080).
- ✅ 289 CPM-topics aangemaakt (RF 3, ISR 2,3,1).
- ✅ 3 per-cluster Logstash draaien, verbonden met central management, gefilterd
  per cluster (`["<dc>_*"]`). Ze staan idle met "No configuration found" tot het
  `dc`-prefix is toegepast (stap 3-4).

## Router-Logstash (test-dataset -> juiste topic, anders DLQ)

`logstash-router` is een **standalone** (niet CPM-managed) Logstash die:
1. leest uit het Kafka-topic **`test-dataset`**;
2. per event het doel-topic bepaalt als
   `<data_stream.type>-<data_stream.dataset>-<data_stream.namespace>`;
3. het event naar dat topic schrijft **als het bestaat**, anders naar het topic
   **`dead-letter-queue`** (met een `[dlq]`-veld: `intended_topic` + `reason`).

"Bestaat het topic?" wordt bepaald met een `translate`-filter tegen
`config/logstash-router/valid_topics.yml` — een woordenboek van bestaande
Kafka-topics dat elke 60s wordt herladen. Regenereren na het toevoegen van
topics:

```bash
docker exec dod-elastic-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list | grep -vE '^__' | sort \
  | sed -E 's/.*/"&": "yes"/' > config/logstash-router/valid_topics.yml
```

De topics `test-dataset` en `dead-letter-queue` worden aangemaakt door
`scripts/create_kafka_topics.py` (die ze altijd meeneemt).

`test-dataset` vullen met de eerder gegenereerde testdata (1 event per data
stream uit de stack-monitoring van de afgelopen 24u):

```bash
python3 scripts/seed_test_dataset.py --insecure          # of --dry-run
```

Elk event krijgt een `data_stream`-object dat uit de data-stream-naam wordt
afgeleid; de router routeert het naar het bijbehorende topic of naar de DLQ als
dat topic niet bestaat.

**Geverifieerd** — 6 testevents in `test-dataset`: 4 met bestaand doel-topic
(`logs-system.auth-default`, `logs-winlog.winlog-prd`,
`metrics-nginx.stubstatus-default`, `logs-nginx-prod`) kwamen elk correct aan; 2
onbekende (niet-bestaand dataset + ontbrekende `data_stream`) belandden in
`dead-letter-queue`.

## Kafka vullen met volume (load)

`scripts/fill_kafka_events.py` produceert events rechtstreeks in de topics met
`kafka-producer-perf-test.sh` (parallel), zonder de router te triggeren:

```bash
python3 scripts/fill_kafka_events.py            # 1.000.000 random verdeeld + 4x 200.000
python3 scripts/fill_kafka_events.py --dry-run  # toon het plan
```

Geverifieerd: 1.800.000 events geproduceerd — ~3.400 per topic over 310 topics,
en 4 gekozen topics elk ~203.000.

## Bekende aandachtspunten / vervolgstappen

1. **Central-management-rechten.** De Logstash-config gebruikt `elastic` voor
   `xpack.management` (net als de monitoring-sectie). De `logstash_system`-user
   mist `manage_logstash_pipelines` → gaf 403 op `_logstash/pipeline`. Voor
   productie: een dedicated user met de `logstash_admin`-rol i.p.v. `elastic`.
2. **Logstash 9.4 setting.** `http.host` bestaat niet meer in 9.4 → vervangen door
   `api.http.host`. (De bestaande `logstash-beats`/`logstash-managed` hadden nog
   `http.host` en waren daardoor al gestopt.)
3. **ES-write-path van de pipelines nog niet volledig bedraad (bestaand).** De
   CPM-pipeline schrijft naar `__ES_HOSTS__` (= registry `ingest_hosts`) met
   API-key-env-var `ES_API_KEY_<CLUSTER_UUID>`. Nu zijn de `ingest_hosts`
   container-ID's (geen `https://…:9200`-URL) en zijn er geen per-cluster
   ingest-API-keys in de Logstash-containers. Voor échte doorstroom
   Kafka→Logstash→ES is nog nodig:
   - `ingest_hosts` → resolvbare ES-URL per cluster (bijv. `https://es-central:9200`);
   - per-cluster ingest-API-keys aanmaken en als `ES_API_KEY_<UUID>` aan de juiste
     Logstash-container meegeven.
4. **Geheugen.** Zie de sectie "Opstartvolgorde, persistentie & geheugen"
   hierboven: totale footprint ~14,6 GB. Pas `KAFKA_HEAP_OPTS`/`mem_limit` aan
   bij een kleinere Docker-VM.

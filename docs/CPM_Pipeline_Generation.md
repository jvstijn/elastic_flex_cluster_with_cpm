---
title: "CPM Pipeline-generatie"
subtitle: "Dedicated en catchall Logstash-pipelines in een dual-Kafka-omgeving"
author: "Cluster Pipeline Manager (elastic_cpm)"
date: "juli 2026"
---

# CPM Pipeline-generatie

Hoe de Cluster Pipeline Manager Logstash-pipelines bepaalt, rendert en uitrolt wanneer ingest via twee Kafka-clusters (`dhl` en `mld`) naar dezelfde Elasticsearch-doelclusters loopt.

## Documentsamenvatting

| Veld | Waarde |
|:-----|:-------|
| Deployment | Ansible-role `elastic_cpm` op kaposi.net |
| Centrale Elasticsearch | `https://central.kaposi.net` |
| Kafka — DHL | `dhl-kafka:9092` |
| Kafka — MLD | `mld-kafka:9092` |
| Watcher webhook-host | `es-central-01` |
| State-manager schema | Dagelijks om 00:15 UTC |
| Pipeline-manager schema | Dagelijks om 00:20 UTC |

## Belangrijkste ontwerpkeuzes

1. **Eén state-document per stream** — sleutel: `{type}-{dataset}-{namespace}`. Geen `kafka_cluster`-veld in state.
2. **Catchall = twee Logstash-pipelines per ES-cluster** — één leest van DHL, één van MLD. Zelfde topiclijst, zelfde ES-output.
3. **Dedicated = één Logstash-pipeline per gepromoveerde stream** — twee Kafka-inputblokken (DHL + MLD) in één pipeline.
4. **Zelfde consumer group over Kafka-clusters** — bijv. `cpm-cluster01` op zowel dhl als mld.
5. **Dedicated topics uitgesloten van beide catchalls** — bij promotie van een stream verdwijnt die uit de topiclijsten van zowel dhl- als mld-catchall.

---

# 1. Overzicht

CPM gebruikt twee Elasticsearch-watchers met elk een eigen verantwoordelijkheid:

| Watcher | Verantwoordelijkheid |
|:--------|:---------------------|
| `cpm-state-manager` | Berekent de *gewenste* routing: welke streams dedicated vs catchall zijn, en welk ES-cluster ze ontvangt. Schrijft naar `cpm-pipeline-state`. |
| `cpm-pipeline-manager` | Leest gewenste state, rendert Logstash-pipelineconfiguraties vanuit templates en pusht ze naar `/_logstash/pipeline`. |

Een derde watcher, `cpm-routing-advisor`, draait stroomopwaarts. Die analyseert ingest-snelheden en clusterscores en schrijft suggesties naar `cpm-routing-suggestions`. De state-manager verwerkt die suggesties; de pipeline-manager leest suggesties niet rechtstreeks.

**Belangrijk:** de pipeline-manager rendert alleen pipelines. Hij bepaalt geen routingbeleid.

---

# 2. End-to-end flow

De dagelijkse CPM-keten verloopt in de volgende volgorde.

## Stap 1 — Routingadvies

`cpm-routing-advisor` inspecteert monitoringdata en clusterscores en suggereert welke streams met hoog volume een dedicated pipeline verdienen of naar een lichter cluster verplaatst kunnen worden.

**Output-index:** `cpm-routing-suggestions`

## Stap 2 — Gewenste staat

`cpm-state-manager` leest routing-suggesties, routing-locks, stream-uitsluitingen en bestaande state. Hij schrijft één document per stream naar `cpm-pipeline-state`, gelabeld als `dedicated` of `catchall`.

**Output-index:** `cpm-pipeline-state`

## Stap 3 — Pipeline renderen en pushen

`cpm-pipeline-manager` leest `cpm-pipeline-state`, het clusterregister en pipeline-templates. Hij rendert Logstash-configuraties en:

- `PUT` nieuwe/bijgewerkte pipelines naar `/_logstash/pipeline/{pipeline_id}`
- `DELETE` inactieve of legacy-pipelines

**Output:** centraal beheerde Logstash-pipelines op de reference stack

## Stap 4 — Ingest-uitvoering

Logstash leest van Kafka en schrijft naar het doel-ES-cluster dat in `cpm-cluster-registry` staat (`ingest_hosts`).

| Bron | Bestemming |
|:-----|:-----------|
| `dhl-kafka:9092` | Doel-ES-cluster (via catchall-dhl of dedicated pipeline) |
| `mld-kafka:9092` | Zelfde doel-ES-cluster (via catchall-mld of dedicated pipeline) |

Aan de Elasticsearch-kant is er geen onderscheid — beide Kafka-clusters voeden hetzelfde cluster.

---

# 3. Dedicated vs catchall

## 3.1 Vergelijking

| Aspect | Dedicated pipeline | Catchall pipeline |
|:-------|:-------------------|:------------------|
| Wanneer gebruikt | Stream gepromoveerd door routing advisor (hoog eventvolume) | Alle streams die niet dedicated zijn |
| State-documenten | 1 per stream (`pipeline_type: dedicated`) | 1 per stream (`pipeline_type: catchall`) |
| Aangemaakte Logstash-pipelines | **1** per gepromoveerde stream/cluster | **2** per ES-cluster (dhl + mld) |
| Kafka-inputs per pipeline | 2 (dhl en mld in dezelfde config) | 1 (dhl of mld) |
| Geconsumeerde topics | Eén topic, gelezen van beide Kafka-clusters | Alle niet-dedicated topics voor dat cluster |
| Verwijderd uit catchall | Ja — uit zowel dhl- als mld-catchall | — |
| Pipeline ID-patroon | `{dc}_cpm-dedicated-{cluster_id}` | `{dc}_cpm-catchall-{dhl\|mld}-{cluster_id}` |
| Templatebestand | `cpm-pipeline-template-dedicated.json` | `cpm-pipeline-template-catchall.json` |

## 3.2 Voorbeelden op kaposi

### cluster01 — catchall (80 datasets)

| Pipeline ID | Kafka bootstrap | Opmerking |
|:------------|:----------------|:----------|
| `dc-01_cpm-catchall-dhl-IY9RlbVgQHqjXoVEfjImBg` | `dhl-kafka:9092` | Alle catchall-topics voor cluster01 |
| `dc-01_cpm-catchall-mld-IY9RlbVgQHqjXoVEfjImBg` | `mld-kafka:9092` | Zelfde topiclijst als dhl-variant |

### cluster04 — dedicated (één gepromoveerde stream)

| Pipeline ID | Kafka bootstrap | Topic |
|:------------|:----------------|:------|
| `dc-04_cpm-dedicated-ByFVGWSTR6y1F4mCgXHoVg` | `dhl-kafka:9092` + `mld-kafka:9092` | `logs-worker-jobs-prd.03` |

Voorbeeld consumer group: `cpm-cluster04` (identiek op beide Kafka-inputs).

---

# 4. cpm-pipeline-manager

**Bronbestand:**

`ansible/roles/elastic_cpm/templates/watcher_cpm-pipeline-manager.json.j2`

## 4.1 Inputs

| Inputnaam | Elasticsearch-index | Doel |
|:----------|:--------------------|:-----|
| `state` | `cpm-pipeline-state` | Gewenste routing per stream (query size: 3000) |
| `registry` | `cpm-cluster-registry` | `ingest_hosts`, `dc`, `active`, `cluster_name` |
| `template_dedicated` | `cpm-pipeline-templates` | Dedicated Logstash-skelet (`_id: dedicated`) |
| `template_catchall` | `cpm-pipeline-templates` | Catchall Logstash-skelet (`_id: catchall`) |

## 4.2 Transform-logica

De watcher groepeert state-entries op `pipeline_id` en vertakt daarna op `pipeline_type`:

- **`dedicated`** — rendert één pipeline met dubbele Kafka-inputs
- **`catchall`** — rendert twee pipelines (dhl en mld) met dezelfde topiclijst

Legacy catchall-pipelines met één Kafka (`{dc}_cpm-catchall-{cluster_id}`) worden in de wachtrij gezet voor verwijdering zodra een catchall-groep verwerkt wordt.

## 4.3 Acties

| Actie | Methode | Pad | Limiet |
|:------|:--------|:----|:-------|
| `push_pipelines` | PUT | `/_logstash/pipeline/{pipeline_id}` | max 100 iteraties |
| `delete_inactive_pipelines` | DELETE | `/_logstash/pipeline/{pipeline_id}` | max 100 iteraties |

Beide acties authenticeren via de `cpm-watcher-webhook` API key en roepen `es-central-01:9200` aan binnen het Docker-netwerk.

---

# 5. Pipeline-generatiecode

De generatielogica staat in het Painless-transformscript in `watcher_cpm-pipeline-manager.json.j2`.

## 5.1 Dedicated-tak

Rendert **één** pipeline met **twee** Kafka-inputblokken:

```text
if ('dedicated'.equals(ptype)) {

  // Bepaal topic uit state-entry (+ omgevingsextensie, zie 5.3)
  String topic = streamType + '-' + dataset + '-' + namespace + topicExt;

  // Vervang template-placeholders
  config = dedicatedTpl
    .replace('__KAFKA_BOOTSTRAP_DHL__', 'dhl-kafka:9092')
    .replace('__KAFKA_BOOTSTRAP_MLD__', 'mld-kafka:9092')
    .replace('__TOPIC__', topic)
    .replace('__GROUP_ID__', 'cpm-' + clusterName)
    .replace('__ES_HOSTS__', ingestHosts)
    .replace('__DATASET__', dataset)
    .replace('__NAMESPACE__', namespace)
    .replace('__API_KEY_VAR__', apiKeyVar);

  // Pipeline ID: {dc}_cpm-dedicated-{cluster_id}
  pipelines.add({ pipeline_id: pid, body: renderedConfig });
}
```

## 5.2 Catchall-tak

Rendert **twee** pipelines — één per Kafka-cluster:

```text
} else {

  // Verzamel en dedupliceer alle topics voor deze pipeline_id; elk topic
  // krijgt de omgevingsextensie erachter (zie 5.3)
  Set topicSet = ...;
  String topicsList = '"logs-foo-bar", "metrics-baz-qux", ...';

  for (kafkaSuffix in ['dhl', 'mld']) {

    String outPid = dc + '_cpm-catchall-' + kafkaSuffix + '-' + clusterId;

    config = catchallTpl
      .replace('__KAFKA_BOOTSTRAP__', kafkaBootstrap)   // dhl of mld
      .replace('__TOPICS_LIST__', topicsList)
      .replace('__GROUP_ID__', 'cpm-' + clusterName)
      .replace('__ES_HOSTS__', ingestHosts)
      .replace('__API_KEY_VAR__', apiKeyVar);

    pipelines.add({ pipeline_id: outPid, body: renderedConfig });
  }

  // Verwijder legacy catchall met één Kafka
  inactivePipelineIds.add(dc + '_cpm-catchall-' + clusterId);
}
```

## 5.3 Topic-extensie per omgeving

Acceptatie en productie delen dezelfde Kafka, maar niet dezelfde topics: in
acceptatie eindigt elke topicnaam op `-acc`, in productie heeft hij geen
extensie. De watcher plakt die extensie zelf achter elke naam die hij rendert —
zowel achter `__TOPIC__` (dedicated) als achter elk topic in `__TOPICS_LIST__`
(catchall).

De waarde komt uit het template-document, net als `kafka_bootstrap_dhl`:

```text
def kte = hit._source.kafka_topic_extension;
if (kte != null) topicExt = (String)kte;
...
if (!topicExt.isEmpty() && !t.endsWith(topicExt)) { t = t + topicExt; }
```

Wie die waarde zet: Ansible, bij het seeden van de templates
(`tasks/seed_data.yml`), uit `kafka_topic_extension` in de inventory.

| inventory | groep | `kafka_topic_extension` | topic |
|---|---|---|---|
| `inventories/acc` | `acc` | `-acc` | `logs-nginx-prod-acc` |
| `inventories/kaposi` | `prd` | `""` | `logs-nginx-prod` |
| `inventories/local` | — | role-default `""` | `logs-nginx-prod` |

Omdat de extensie in het document staat en niet in de watcher zelf, kun je hem
runtime wijzigen met een `PUT` op `cpm-pipeline-templates` zonder de watchers
opnieuw uit te rollen. De eerstvolgende ansible-run zet hem weer terug naar wat
de inventory zegt.

De data streams in Elasticsearch houden hun kale naam: de extensie zit alleen op
het Kafka-topic, niet op `data_stream_dataset`. `logs-nginx-prod-acc` landt dus
in de data stream `logs-nginx-prod` op een acc-cluster.

De bijbehorende topics maak je met `scripts/create_kafka_topics.py --source
kafka --suffix=-acc`.

---

# 6. Logstash-templates

Templates staan in `cpm-pipeline-templates` en worden door Ansible geseed vanuit de JSON-bestanden hieronder.

## 6.1 Catchall-template

**Bestand:** `ansible/roles/elastic_cpm/files/json/cpm-pipeline-template-catchall.json`

Eén Kafka-input per gerenderde pipeline. Wordt twee keer geïnstantieerd per ES-cluster (dhl en mld).

```ruby
input {
  kafka {
    bootstrap_servers => "__KAFKA_BOOTSTRAP__"
    topics => [__TOPICS_LIST__]
    group_id => "__GROUP_ID__"
    consumer_threads => __CONSUMER_THREADS__
    codec => json
    decorate_events => true
  }
}

output {
  elasticsearch {
    hosts => [__ES_HOSTS__]
    data_stream => true
    api_key => "${__API_KEY_VAR__}"
    ssl_certificate_verification => true
  }
}
```

Metadatavelden op het template:

| Veld | Waarde |
|:-----|:-------|
| `kafka_bootstrap_dhl` | `dhl-kafka:9092` |
| `kafka_bootstrap_mld` | `mld-kafka:9092` |

## 6.2 Dedicated-template

**Bestand:** `ansible/roles/elastic_cpm/files/json/cpm-pipeline-template-dedicated.json`

Twee Kafka-inputs in één pipeline.

```ruby
input {
  kafka {
    bootstrap_servers => "__KAFKA_BOOTSTRAP_DHL__"
    topics => ["__TOPIC__"]
    group_id => "__GROUP_ID__"
    consumer_threads => __CONSUMER_THREADS__
    codec => json
    decorate_events => true
  }
  kafka {
    bootstrap_servers => "__KAFKA_BOOTSTRAP_MLD__"
    topics => ["__TOPIC__"]
    group_id => "__GROUP_ID__"
    consumer_threads => __CONSUMER_THREADS__
    codec => json
    decorate_events => true
  }
}

output {
  elasticsearch {
    hosts => [__ES_HOSTS__]
    data_stream => true
    data_stream_type => "logs"
    data_stream_dataset => "__DATASET__"
    data_stream_namespace => "__NAMESPACE__"
    api_key => "${__API_KEY_VAR__}"
    ssl_certificate_verification => true
  }
}
```

---

# 7. cpm-state-manager (stroomopwaarts)

**Bronbestand:**

`ansible/roles/elastic_cpm/templates/watcher_cpm-state-manager.json.j2`

De state-manager berekent gewenste routing in deze volgorde:

1. **Routing locks** — handmatige overrides uit `cpm-routing-config` (vergrendelde streams behouden toegewezen cluster en pipeline-type).
2. **Dedicated-promotie** — maximaal één dedicated pipeline per cluster per run, op basis van `cpm-routing-suggestions`.
3. **Catchall-toewijzing** — alle overige streams worden aan catchall op hun doelcluster toegewezen.
4. **Uitsluitingsopruiming** — streams die matchen met patronen in `cpm-stream-exclusions` worden uit state verwijderd.

Wanneer een stream naar dedicated wordt gepromoveerd, komt die in `managedTopics` en valt daarmee buiten de topiclijsten van zowel dhl- als mld-catchall bij de volgende pipeline-manager-run.

State-document-ID: `{data_stream_type}-{dataset}-{namespace}`

---

# 8. Bronbestanden

| Artifact | Relatief pad (vanaf `ansible/`) |
|:---------|:-------------------------------|
| Pipeline-renderer watcher | `roles/elastic_cpm/templates/watcher_cpm-pipeline-manager.json.j2` |
| State-writer watcher | `roles/elastic_cpm/templates/watcher_cpm-state-manager.json.j2` |
| Catchall-template | `roles/elastic_cpm/files/json/cpm-pipeline-template-catchall.json` |
| Dedicated-template | `roles/elastic_cpm/files/json/cpm-pipeline-template-dedicated.json` |
| Template-mapping update | `roles/elastic_cpm/tasks/main.yml` |
| Routing-advisor watcher | `roles/elastic_cpm/templates/watcher_cpm-routing-advisor.json.j2` |

---

# 9. ML- en monitoringimpact

Geen wijzigingen aan ML-jobs of datafeeds nodig voor dual-Kafka-ondersteuning.

Alle vijf CPM ML-detectors (`cpm-store-size`, `cpm-jvm-heap`, `cpm-shard-count`, `cpm-cluster-event-rate`, `cpm-event-rate`) lezen geaggregeerde metriek uit `.monitoring-es-8-*` aan de Elasticsearch-kant. Routingbeslissingen en scoring zijn gebaseerd op totale ingest zoals waargenomen in ES, niet op welk Kafka-cluster de data oorspronkelijk vandaan kwam.

## Waar monitoring staat

Monitoring draait op een eigen cluster. Elke Metricbeat scrapet zijn eigen
cluster en schrijft naar dat monitoring-cluster; het centrale cluster leest het
terug via de cross-cluster alias `monitoring:`. Zo komt CPM aan zowel het bestaan
van een stream als aan het cluster waar hij thuishoort.

Twee plekken moeten daarbij op dezelfde index wijzen:

| onderdeel | index | herkomst |
|---|---|---|
| watchers | `monitoring:.monitoring-es-8-*` | hard in de watcher-templates |
| ML-datafeeds en veldprobe | `{{ cpm_monitoring_index }}` | inventory of role-defaults |

Staat `cpm_monitoring_index` zonder het `monitoring:` prefix terwijl monitoring
op een apart cluster draait, dan zoeken de datafeeds lokaal, vinden ze niets en
blijven alle cluster-scores op 0. De watchers draaien dan gewoon door, dus je
ziet geen fout: alleen een weging die nergens onderscheid maakt.

Bij het invoeren van een apart monitoring-cluster in een bestaande omgeving:

1. Zet `cpm_monitoring_index` om in de inventory.
2. Verhuis de historie met `scripts/copy_monitoring_history.py`; zonder historie
   heeft de ML geen model en dus geen forecast.
3. Draai de rol met `-e cpm_ml_reinstall=true`. Een lopende datafeed leest niet
   met terugwerkende kracht, dus zonder herinstallatie blijft de oude historie
   ongebruikt.

---

# 10. Operationele notities

## Wijzigingen deployen

```bash
cd Jan/elastic_flex_cluster_with_cpm/ansible
.venv/bin/ansible-playbook site.yml \
  -e elastic_base_url=https://central.kaposi.net \
  -e webhook_host=es-central-01
```

## Pipeline-refresh handmatig triggeren

```bash
curl -sk -u elastic:$ELASTIC_PASSWORD -X POST \
  'https://central.kaposi.net/_watcher/watch/cpm-pipeline-manager/_execute' \
  -H 'Content-Type: application/json' \
  -d '{"record_execution": true}'
```

## Pipelines voor een cluster verifiëren

```bash
curl -sk -u elastic:$ELASTIC_PASSWORD \
  'https://central.kaposi.net/_logstash/pipeline' | jq 'keys[]' | grep dc-01
```

Verwacht voor een actief catchall-cluster: twee entries die eindigen op `-dhl-` en `-mld-`.

---

*Cluster Pipeline Manager — referentie dual-Kafka pipeline-generatie.*

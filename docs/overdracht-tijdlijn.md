# Overdracht — tijdlijn van wijzigingen (week van 30 juni 2026)

Verslag voor de overdracht: wat is er deze week gewijzigd en waarom.
Periode: **di 30-06** t/m **di 07-07-2026**. Branch: `mod-jan` (up-to-date met `main`, PR-klaar).
(Maandag 29-06 zijn er geen wijzigingen; het werk begon dinsdag.)

## Executive summary

Deze week is de CPM-stack op drie vlakken verbeterd en gehard:

1. **Coverage-checker gebouwd** — een script dat laat zien welke data streams wél/niet
   door een CPM Logstash-pipeline worden gedekt (alleen actieve clusters, laatste 24u).
   Dit bracht twee bugs aan het licht.
2. **Twee state-manager bugs opgelost** (watcher én workflow): de discovery convergeerde
   te traag (dagen i.p.v. één run) en sloeg namespace-loze datasets stil over. Netto
   ging de dekking van **239 → 9 MISSING** in één run-cyclus (de 9 zijn een
   test-data-artefact, geen bug).
3. **Kafka + Logstash-topologie neergezet en gehard**: een 3-broker Kafka-cluster met
   web-GUI, één Logstash per ES-cluster, en een router die `test-dataset` naar het juiste
   topic stuurt (of naar een dead-letter-queue). Daarna operationeel hardened:
   Kafka-**healthchecks** (opstartvolgorde), **`KAFKA_LOG_DIRS`** (data ging eerst
   verloren bij elke recreate), een geheugentrim (~16 → ~14,6 GB) en een crashende
   `logstash-beats` gefixt.

**Stand van zaken:** de stack draait volledig, Kafka is gevuld met ~1,8 mln events
(nu persistent), alles is gecommit/gepusht op branch **`mod-jan`** (0 achter op `main`,
klaar voor een conflictvrije PR). **Belangrijkste openstaande punt:** de daadwerkelijke
doorstroom Kafka→Logstash→ES vereist nog het bedraden van de pipeline-`ingest_hosts`
(nu container-ID's) + per-cluster ingest-API-keys, plus het activeren van `node.attr.dc`.
Verder moet de PR nog aangemaakt worden. Details staan hieronder en in de twee
gelinkte docs.

## 0. Context in één alinea

De stack is een Docker + Ansible Elasticsearch-omgeving (Elastic 9.4.2) met **CPM**
(Central Pipeline Management): een centrale ES (`es-central`) + twee remote clusters
(`remote-a`, `remote-b`). CPM ontdekt automatisch welke data streams er zijn en bouwt
per cluster Logstash-pipelines die Kafka-topics `<type>-<dataset>-<namespace>`
consumeren. CPM bestaat in twee vormen: **watchers** (`cpm-*`, Painless) en een native
**workflow-port** (`cpmw-*`, Liquid). Deze week draaide om (1) een coverage-checker,
(2) twee bugs in de CPM state-manager, en (3) een Kafka-cluster + Logstash-routing +
operationele hardening.

## 1. Twee sporen deze week

- **`mod-jan`** (J.G. van Stijn) — al het onderstaande werk.
- **`main`** (Imre Kaposi) — parallel: migratie van de python-installer naar Ansible
  (`5c5bfb3`), uitrol op `cpm.kaposi.net` (`099a5e9`), en een eigen Painless-fix voor
  de namespace-bug (`730f684`). Op wo 01-07 is `main` in `mod-jan` gemerged
  (`ad11e33`, zie 3.6).

---

## 2. Tijdlijn

### Dinsdag 30-06 — coverage-checker + eerste fixes
| Commit | Wijziging | Waarom |
|---|---|---|
| `76b4c39` | **Coverage-checker** `scripts/check_index_pipeline_coverage.py` | Inzicht: welke indices/data streams worden wél/niet door een CPM Logstash-pipeline geconsumeerd (OK/MISSING) + op welk cluster ze draaien. |
| `8ad44d2` | `cpmw-state-manager`: `filebeat`-topic behouden bij state-normalisatie | Regressie: bij her-runs werd het topic `filebeat` foutief herschreven naar `logs-filebeat-default`. |
| `70c1faa` | Coverage-checker: 24u-venster + rapport ook naar bestand | Alleen recent-actieve indices meenemen; rapport bewaren. |
| `1da3792` | Indexnamen uit stack-monitoring (`monitoring:.monitoring-es-8-mb*`) + owning-cluster tonen | Zo tellen ook indices op remote clusters mee (niet alleen lokaal). |
| `13ffabd` | Alleen indices op **actieve** clusters (`active=true` in `cpm-cluster-registry`) | Indices op gedeactiveerde/onbekende clusters horen niet in de dekking. |
| `fc2d3f9` | **Testdata-generator** `scripts/gen_monitoring_testdata.py` | Synthetische monitoring-docs per dataset om de checker te voeden (4 datasets groot: 80k–100k). |
| *(main)* `730f684` | Imre: Painless-fix namespace-bug | Parallel spoor; later verwerkt in de merge (zie hieronder). |

### Woensdag 01-07 — bugfixes + Kafka/Logstash-infra
| Commit | Wijziging | Waarom |
|---|---|---|
| `82aaffb` | Testdata **verdeeld** over de 3 actieve clusters (round-robin) + 4 grote bewust verdeeld | Realistischer testbeeld; overlap-vrij (elke dataset op één cluster). |
| `accad86` | **Fix CPM state-manager discovery** (watcher + cpmw-workflow) | Twee bugs, zie hoofdstuk 4. Kern van de week. |
| `863ce99` | **3-broker Kafka-cluster + kafka-ui (GUI) + 1 Logstash per ES-cluster** | Realistische topologie: elk cluster een eigen Logstash die via een `dc`-prefix alleen zijn eigen pipelines draait. |
| `3e7f03e` | `scripts/cpm_run_now.py` | De CPM-cyclus draait 1×/dag (cron); dit script triggert 'm handmatig op elk moment. |
| `ad11e33` | **Merge `main` → `mod-jan`** (3 conflicten opgelost) | `main` was vooruitgelopen (Ansible-migratie, kaposi-deploy, eigen namespace-fix). |
| `5acba52` | **`logstash-router`**: leest `test-dataset`, routeert naar `<type>-<dataset>-<namespace>`, onbekend topic → `dead-letter-queue` | Gevraagde routing-demo met DLQ. |
| `5721801` | `scripts/seed_test_dataset.py` | Vult `test-dataset` met de gegenereerde 24u-data (1 event per dataset). |
| `eb0addc` | `scripts/fill_kafka_events.py` | Load: 1.000.000 events random over de topics + 4 topics × 200.000. |

### Donderdag 02-07 — operationele hardening
| Commit | Wijziging | Waarom |
|---|---|---|
| `f4b355e` | **Kafka-healthchecks** + clients wachten op `service_healthy` | Opstartvolgorde-bug: clients (kafka-ui, alle Logstash) connecteerden vóórdat Kafka klaar was → connection-errors in het ~30s quorum-venster. |
| `2ec8005` | **Matige geheugentrim** + `logstash-beats` fix | Footprint ~16,2 → ~14,6 GB; `logstash-beats` crashte al 5 dagen op de 9.4-setting `http.host` → `api.http.host`. |
| `311d709` | **`KAFKA_LOG_DIRS`** → gemounte volume | Latente databug: zonder deze setting schreef Kafka naar ephemeral `/tmp` i.p.v. de volume → data ging verloren bij elke container-recreate. |
| `0ae4378` | Router-woordenboek (`valid_topics.yml`) ververst | Na herbouw van de topics. |
| `8152a65` | Documentatie bijgewerkt | Healthchecks, persistentie, geheugen vastgelegd. |

### Vrijdag 03-07 — routing-overrides getest + rename
| Commit | Wijziging | Waarom |
|---|---|---|
| `f99e776` | Ansible-variabele `kibana_base_url` → `kibana_url` (7 bestanden) | Consistente naamgeving. |
| `ae4a101` | `docs/runbook-stream-routing.md` — **force/exclude runbook** | Productie-stappen om een data stream naar een ander cluster te **forceren** (stream-lock in `cpm-routing-config`) of **uit te sluiten** (`cpm-stream-exclusions`). Live getest: force central→remote-a én exclude, beide teruggedraaid. |
| `c45076f` | testresultaat toegevoegd | — |
| *(main)* `78bae40`, `493bd30`, `8c3d868` | pipeline-manager delete-fix + `cpm-stream-coverage` watcher | Parallel main-spoor. |

Twee productie-inzichten uit de test: de state is **sticky** (een lock/exclusion weghalen
zet niet automatisch terug) en een `dc`-wijziging laat **oude `default_cpm-*` pipelines**
staan die dubbel consumeren. Beide staan in de runbook.

### Dinsdag 07-07 — duplicaat-check + bijgewerkt tot main
| Commit | Wijziging | Waarom |
|---|---|---|
| `33fa71a` | Coverage-checker: **detectie van dubbele topics** | Een topic mag maar in **één** pipeline; live **22 duplicaten** gevonden (cpm ↔ cpmw overlap = dubbele ingest). Exit-code ≠ 0 bij dubbel/missend. |
| `01f862a` | **Merge `origin/main` → `mod-jan`** (2 conflicten opgelost) | main was 9 commits vooruit (o.a. hardening `746b364`, `cpm-stream-coverage` watcher). `mod-jan` nu **0 achter**, PR-klaar. |

---

## 3. De belangrijkste wijzigingen, inhoudelijk

### 3.1 Coverage-checker (`scripts/check_index_pipeline_coverage.py`)
Vergelijkt indices (uit stack-monitoring, 24u, alleen `active=true` clusters) met de
topics die de CPM Logstash-pipelines consumeren. Toont per index: owning-cluster, topic,
status (OK/MISSING) en welke pipeline(s). Was het diagnose-instrument dat de twee bugs
hieronder aan het licht bracht.

### 3.2 De twee state-manager bugs (commit `accad86`) — kern
Gedetailleerd in **`docs/CPM-coverage-en-state-manager-bevindingen.md`**. Samengevat:
- **Bug A (trage convergentie):** de discovery sleutelde intern op `type|dataset`
  (zonder namespace/cluster) → per run werd maar ~1 namespace per dataset-familie
  ontdekt. Met een dagelijkse cron duurde volledige dekking dagen. → sleutel op de
  volledige topic → alles in **1 run**. Ook `max_iterations` 200 → 1000.
- **Bug B (namespace-loze datasets):** `if (nd <= 0) continue` sloeg namen zonder
  `-namespace` stil over (o.a. `logs-vmware`, `metrics-endpoint.metadata_current_default`).
  → `ns=default`, `topic=<data-stream-naam>`.
- **Effect:** coverage MISSING **239 → 9** in één run-cyclus (de resterende 9 zijn een
  test-data-artefact van "kale + `-default`" duplicaten).
- Toegepast in **beide**: de watcher én de workflow. **Eindstand na de merge met main
  (07-07):** de watcher draait op main's geharde versie — die lost Bug A al op via een
  `type|dataset|namespace`-sleutel en heeft `max_iterations: 16384`; daarop is alleen onze
  **Bug B**-fix opnieuw toegepast. De cpmw-workflow houdt onze Bug B (`keep >= 2`).

### 3.3 Kafka + Logstash-topologie (`863ce99`)
- **3-broker KRaft-cluster** (`kafka`/`kafka2`/`kafka3`), RF 3, `min.insync.replicas=2`.
- **kafka-ui** (GUI) op `http://localhost:8080`.
- **1 CPM-managed Logstash per ES-cluster** (`logstash-central/-remote-a/-remote-b`),
  gefilterd via `node.attr.dc` → pipeline-id-prefix `<dc>_cpm-*`. Vervangt de oude
  `logstash-managed`.
- Details + activatiestappen in **`docs/kafka-logstash-setup.md`**.

### 3.4 Router + DLQ (`5acba52`)
`logstash-router` consumeert `test-dataset`, bepaalt het doel-topic uit
`data_stream.type/dataset/namespace` en produceert daarheen; bestaat het topic niet
(check via `translate`-woordenboek), dan gaat het naar `dead-letter-queue` (met
`[dlq].intended_topic` + `reason`). Geverifieerd: routeerbaar → juiste topic,
onbekend → DLQ.

### 3.5 Operationele fixes (do 02-07)
De crashes die zichtbaar waren, hadden **twee** oorzaken die nu beide zijn opgelost:
1. **Opstartvolgorde** — Kafka had geen healthcheck; clients connecteerden te vroeg.
2. **Persistentie** — `KAFKA_LOG_DIRS` ontbrak; Kafka-data stond ephemeral en ging bij
   elke recreate verloren.
Let op: wat eerst als "OOM" leek, was géén geheugen-OOM (`OOMKilled=false`, Docker-VM
heeft 34 GB) maar een `docker compose`-recreate die de containers neerhaalde.

### 3.6 Merges met `main` (`ad11e33`, `01f862a`) + duplicaat-check
Twee keer gemerged. Bij de **tweede** (07-07) was main fors vooruit (hardening `746b364`:
home-cluster-rehoming, stale-simulator-cleanup, `max_iterations: 16384`). Resolutie: main's
geharde `watcher_cpm-state-manager.json.j2` als basis + onze **Bug B** eroverheen; en
`tasks/dashboard.yml` = main's versie + de `kibana_url`-rename. Onze 3-broker
`kafka_bootstrap` en de cpmw Bug B mergeden automatisch mee. `mod-jan` is nu 0 achter main.

De coverage-checker meldt sinds `33fa71a` ook **dubbele topics** (een topic in >1 pipeline);
live gaf dat 22 hits door de parallelle cpm/cpmw-pipelines.

---

## 4. Huidige staat (di 07-07)

- **Stack draait** (alles Up/healthy): 3× ES, 3× Kafka (healthy), Kibana, kafka-ui,
  5× Logstash (beats/central/remote-a/remote-b/router), 3× metricbeat.
- **Kafka gevuld**: ~1.800.000 events, 4 grote topics ~203k, nu **persistent** op de
  volumes.
- **Geheugen**: footprint ~14,6 GB (Docker-VM 34 GB).
- **Coverage-checker**: detecteert nu ook **dubbele topics** (topic in >1 pipeline).
- **Git**: `mod-jan` is **bijgewerkt tot `main`** (0 commits achter, 23 vooruit), alles
  gepusht — klaar voor een conflictvrije PR → `main`.

## 5. Openstaande punten / vervolgstappen

1. **PR `mod-jan` → `main` nog aanmaken** (geen `gh`/token op de machine; open de
   compare-link `github.com/jvstijn/elastic_flex_cluster_with_cpm/compare/main...mod-jan?expand=1`).
   `mod-jan` is up-to-date met `main`.
2. **CPM-write-path niet volledig bedraad**: de per-cluster Logstash draaien pas hun
   pipelines nadat `node.attr.dc` is geactiveerd (recreate ES + `register-sync`), en de
   pipeline-`ingest_hosts` zijn nu container-ID's i.p.v. `https://…:9200`-URL's + er
   zijn nog geen per-cluster `ES_API_KEY_<uuid>`. Nodig voor échte doorstroom
   Kafka→Logstash→ES. Zie `docs/kafka-logstash-setup.md` §"Bekende aandachtspunten".
3. **9 datasets blijven MISSING** in de coverage — test-data-artefact (kale + `-default`
   duplicaten), geen bug. Optioneel op te lossen door de state-doc-`_id` op de volledige
   topic te baseren.
4. **Orphan** `logstash-managed` (oude container) — opruimen met
   `docker compose up -d --remove-orphans`.
5. **`TESTRESULTS-2026-06-26.md`** staat untracked; nog beslissen of dat in git moet.
6. **`ansible/.env`** staat als tracked bestand in de repo (met `*_PASSWORD`-vars; komt van
   `main`, niet van deze branch) — overweeg gitignore + untrack.
7. **Dubbele topics** (22, cpm ↔ cpmw) — in productie draai je normaal één van de twee
   tracks; de `default_cpmw-*` pipelines kunnen uit als je de watchers gebruikt.

## 6. Operationele scripts (voor de nieuwe beheerder)

| Script | Doel |
|---|---|
| `scripts/check_index_pipeline_coverage.py` | Coverage: indices vs pipeline-topics (actieve clusters, 24u). |
| `scripts/cpm_run_now.py` | CPM-cyclus handmatig draaien (register-sync → … → pipeline-manager). |
| `scripts/create_kafka_topics.py` | CPM-topics (+ test-dataset, dead-letter-queue) in Kafka aanmaken. |
| `scripts/gen_monitoring_testdata.py` | Synthetische monitoring-testdata genereren. |
| `scripts/seed_test_dataset.py` | `test-dataset`-topic vullen met de 24u-testdata. |
| `scripts/fill_kafka_events.py` | Kafka bulk vullen (1M random + 4× 200k). |

## 7. Documentatie

- `docs/CPM-coverage-en-state-manager-bevindingen.md` — diepe tijdlijn + bewijs van de
  twee state-manager bugs en de fixes.
- `docs/kafka-logstash-setup.md` — Kafka-cluster, per-cluster Logstash, router/DLQ,
  opstart/persistentie/geheugen, en de bekende aandachtspunten.
- `docs/runbook-stream-routing.md` — productie-stappen om een data stream naar een ander
  cluster te **forceren** of **uit te sluiten** (+ de sticky-state/stale-pipeline caveats).
- **Dit document** — overdracht-tijdlijn op hoofdlijnen.

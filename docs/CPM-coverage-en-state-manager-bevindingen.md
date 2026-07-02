# CPM coverage-onderzoek & state-manager fixes

Tijdlijn met bevindingen en aanpassingen rond het valideren van de CPM-pipeline-dekking
(coverage) en twee bugs die daarbij in de `cpm-state-manager` watcher en de
`cpmw-state-manager` workflow zijn gevonden en opgelost.

Periode: 2026-06-22 t/m 2026-07-01 · branch `mod-jan`

---

## 0. Context

CPM (Central Pipeline Management) bouwt op de centrale Elasticsearch automatisch
Logstash-pipelines die Kafka-topics `<type>-<dataset>-<namespace>` consumeren en naar
de juiste data streams routeren. De keten bestaat uit:

- **Watchers** (`cpm-*`, rol `elastic_cpm`) — Painless-transforms.
- **Native workflows** (`cpmw-*`, rol `elastic_cpm_workflows`) — Liquid + data-steps (Elastic 9.4).

De `state-manager` (watcher én workflow) ontdekt actieve datasets uit de
stack-monitoring (`monitoring:.monitoring-es-8-*`), bepaalt per cluster welke topics naar
welke catchall/dedicated-pipeline moeten, en schrijft dat naar `cpm-pipeline-state` /
`cpmw-pipeline-state`. De `pipeline-manager` bouwt daaruit de Logstash-pipelines.

---

## 1. Tijdlijn

### Fase A — Coverage-checker (`scripts/check_index_pipeline_coverage.py`)
- Script gemaakt dat alle Elasticsearch-indices/data streams vergelijkt met de topics die
  de CPM Logstash-pipelines consumeren, en per index toont: owning-cluster, topic, status
  (OK/MISSING) en welke pipeline(s) het topic lezen.
- Indexnamen + owning-cluster worden uit `monitoring:.monitoring-es-8-mb*` gelezen
  (24h-venster), zodat ook remote clusters meetellen. Rapport naar scherm én bestand.
- **Aanpassing (op verzoek):** alleen indices meenemen die draaien op een cluster dat
  `active=true` is in `cpm-cluster-registry` (nieuwe functie `active_clusters()` +
  `--registry-index`). Live geverifieerd: central op `active=false` → de 24 indices vielen
  correct naar 0. Commit `13ffabd`.

### Fase B — Testdata genereren (`scripts/gen_monitoring_testdata.py`)
- Generator die per dataset één synthetisch `event.dataset: elasticsearch.index`
  monitoring-document in de `.monitoring-es-8-mb` data stream schrijft (dezelfde bron die
  de coverage-checker leest), met `@timestamp` ~5-35 min geleden (binnen 24h).
- ~301 datasets (de door de gebruiker aangeleverde lijst), waarvan 4 groot (80.000-100.000
  docs). Commit `fc2d3f9`.
- **Verdeling over 3 actieve clusters** (round-robin central/remote-a/remote-b; 4 grote
  bewust verdeeld: 1 central, 1 remote-a, 2 remote-b). Marker `.ds-*-2000.01.01-000001`
  zodat de generator z'n eigen docs idempotent opruimt en echte monitoring nooit raakt.
  Commit `82aaffb`.

### Fase C — Overlap verwijderen
- Bevinding: 5 datasetnamen bestonden zowel synthetisch (op een remote cluster) als echt
  (data stream op central) → die indices verschenen op 2 clusters (overlap):
  `logs-elastic_agent.filebeat-default`, `logs-endpoint.alerts-default`,
  `logs-endpoint.events.network-default`, `logs-endpoint.events.process-default`,
  `logs-logstash.log-default`.
- **Aanpassing:** de 5 echte data streams verwijderd + hun monitoring gepurged, daarna
  volledig synthetische set opnieuw verdeeld. Resultaat: 0 overlap, elke dataset op precies
  één cluster (central 120 / remote-a 98 / remote-b 102).

### Fase D — "Waarom 239 MISSING?" — het onderzoek
Na het draaien van de coverage-checker: **239 van de 319 indices MISSING** (geen pipeline).
Onderzoek wees uit dat dit **twee oorzaken** had — deels timing, deels een echte bug:

1. **Timing (geen bug).** De `cpm-state-manager` draait op een **dagelijkse cron**
   (`0 15 0 * * ?`). De testdata was ná de dagrun herverdeeld, dus `cpm-pipeline-state` was
   verouderd. De grootte-cap `by_index size: 500` was níet het probleem (max ~119 backing
   indices per cluster).

2. **Bug A — trage convergentie / sleutel-botsing.** Zelfs bij herhaald draaien voegde de
   discovery per run maar ~één namespace per `(type,dataset)`-familie toe, omdat
   `datasetClusterMap` op `streamType + '|' + dataset` werd gesleuteld — **zonder namespace
   en zonder cluster**. Datasets met veel namespaces (tst/prd/ont/default/acc) hadden zo
   ~5 dagrun­nen nodig voor volledige dekking. Gemeten: 81 → 193 → 249 → 300 → 305 over 4 runs.

3. **Bug B — namespace-loze datasets permanent overgeslagen.** Na convergentie bleven 15
   datasets permanent MISSING. Oorzaak in de discovery-parser:
   ```painless
   String rest = idx.substring(fd + 1);              // bv. "database" of "endpoint.metadata_current_default"
   int nd = rest.lastIndexOf('-'); if (nd <= 0) continue;   // geen 2e '-' → STIL overgeslagen
   ```
   Elke naam zonder `-<namespace>`-segment werd genegeerd: `logs-vmware`, `logs-database`,
   `logs-extranet`, `logs-logmanagement`, `logs-elastic_agent`, de kale `logs-endpoint.events.*`
   en — belangrijk — `metrics-endpoint.metadata_current_default` (een reëel Elastic-index
   waarvan de namespace met `_` i.p.v. `-` aan de dataset vastzit).

   In de **workflow** zat dezelfde Bug B als de guard `if keep >= 3` (regels `topic` en
   `_keep`), plus de dataset/namespace-parsing die 2-delige topics niet aankon. Bug A was in
   de workflow **afwezig**: die dedupliceert al op de volledige topic
   (`mon_pairs` → `data.dedupe keys: [topic]`), dus daar worden alle varianten in één run
   meegenomen.

---

## 2. Aanpassingen (de fixes)

### 2.1 Watcher — `ansible/roles/elastic_cpm/templates/watcher_cpm-state-manager.json.j2`

**Bug A — discovery-map op volledige topic i.p.v. `type|dataset`:**
- `datasetClusterMap.put(streamType + '|' + dataset, dm)` → `datasetClusterMap.put(topic, dm)`;
  `dm` draagt nu ook `dataset` en `topic`.
- Filebeat-tak: sleutel `'filebeat|filebeat'` → `'filebeat'`.
- De merge leest `cluster_id` + `topic` rechtstreeks uit `dm` (geen reconstructie meer).
- Effect: **alle** datasets van alle clusters/namespaces worden in **één** run opgepikt.

**Bug B — namespace-loze datasets niet meer overslaan:**
```painless
int nd = rest.lastIndexOf('-');
String dataset; String ns;
if (nd <= 0) { dataset = rest; ns = 'default'; } else { dataset = rest.substring(0, nd); ns = rest.substring(nd + 1); }
String topic = idx;   // topic == de echte data-stream-naam
```
- De catchall-entry-parser leidt het type nu af uit de prefix en handelt single-dash
  topics af (`metrics-endpoint.metadata_current_default` → type=metrics, ns=default).

**Companion-fix — `max_iterations`:** de `index_state`-actie stond op `200`. Doordat één run
nu ~319 entries oplevert (i.p.v. ~56), werd 200 de nieuwe bottleneck → verhoogd naar `1000`.

### 2.2 Workflow — `ansible/roles/elastic_cpm_workflows/files/workflows/cpmw-state-manager.yml`

**Bug B** (Bug A was hier al afwezig):
- `mon_pairs_pre` `topic` en `_keep`: guard `keep >= 3` → `keep >= 2` (2 = type+dataset,
  namespace optioneel).
- `mon_entries` én `existing_entries` dataset-parsing: 2-delige topics afgehandeld
  (`dataset = parts[1]`).
- namespace-parsing: `parts.size <= 1` → `<= 2` → `default` bij ontbrekende namespace.

### 2.3 Deploy
- Watcher: `ansible-playbook site.yml` (regenereert de webhook-API-key + plaatst de watchers).
- Workflow: `ansible-playbook workflows.yml` (`cpmw-state-manager` valid=True).

---

## 3. Verificatie (voor → na)

| Meting | Voor fix | Na fix (één run-cyclus) |
|---|---|---|
| State-entries per run (watcher) | ~56 (incrementeel) | **319** |
| Runs tot convergentie | ~4-5 (dagelijks = dagen) | **1** |
| Namespace-loze datasets ontdekt | 0 (permanent overgeslagen) | **allemaal** |
| Coverage: covered / MISSING | 24 / **239** | 310 / **9** |
| Workflow `cpmw-pipeline-state` docs (één run) | n.v.t. | **309** |

Concreet geverifieerd:
- Watcher-transform levert 319 entries in één run; `index_state` schrijft alle 319
  (`iterations executed: 319`).
- `metrics-endpoint.metadata_current_default`, `logs-vmware`, `logs-database`,
  `logs-extranet`, `logs-logmanagement` c.s. staan nu in de state.
- Workflow: `cpmw-pipeline-state` = 309 docs na één run, inclusief de namespace-loze datasets.

---

## 4. Bekende restpunten

- **9 datasets nog MISSING** na de fix — dit is een **test-data-artefact**, geen bug in de
  fix. De aangeleverde lijst bevat zowel kale namen (`logs-endpoint.events.file`) als hun
  `-default`-variant (`logs-endpoint.events.file-default`). Beide krijgen dezelfde
  state-doc-`_id` (`<type>-<dataset>-<namespace>` = `logs-endpoint.events.file-default`),
  dus één van de twee overschrijft de ander. In een echte Elastic-omgeving bestaan kale
  data-stream-namen niet (een data stream heeft altijd een namespace), dus dit treedt
  daar niet op. Optioneel op te lossen door de state-doc-`_id` op de volledige `topic` te
  baseren i.p.v. `type-dataset-namespace`.
- **2 orphan topics** (`filebeat`, `logs-beats-raw`) — topics in een pipeline zonder actieve
  index; verwacht, want daar is geen testdata voor.
- De watcher-runs in dit onderzoek zijn handmatig met `force_execute` versneld (de
  dagelijkse cron zou hetzelfde over dagen doen).

---

## 5. Gewijzigde bestanden

| Bestand | Wijziging |
|---|---|
| `scripts/check_index_pipeline_coverage.py` | coverage-checker + active-cluster-filter |
| `scripts/gen_monitoring_testdata.py` | testdata-generator, verdeeld over 3 clusters |
| `ansible/roles/elastic_cpm/templates/watcher_cpm-state-manager.json.j2` | Bug A + Bug B + `max_iterations` |
| `ansible/roles/elastic_cpm_workflows/files/workflows/cpmw-state-manager.yml` | Bug B |
| `docs/CPM-coverage-en-state-manager-bevindingen.md` | dit document |

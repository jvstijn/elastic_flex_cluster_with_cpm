# Testresultaten — CPM watchers + per-cluster Logstash (2026-07-17)

Uitvoering van `docker-local/TESTPLAN.md` tegen de **lokale draaiende setup**,
aangepast aan de huidige stand op branch `mod-imre3006`:

- **3 clusters** (central + remote-a + remote-b) i.p.v. 7, stack **9.4.2**.
- Systeem onder test = de **`cpm-*` watchers** (uitgerold via `ansible-playbook
  site.yml` + `bootstrap.yml`), **niet** de `cpmw-*` workflows.
- **Nieuwe architectuur** t.o.v. de vorige testronde (2026-06-26):
  - Catchall = **gebundeld per cluster** (één pipeline per cluster met de volledige
    topic-lijst, dual-Kafka input, dynamische `data_stream` output).
  - **Per-cluster Logstash** (`logstash-central`/`-remote-a`/`-remote-b`) die elk via
    `xpack.management.pipeline.id`-filter alleen hun eigen `dc-*`-pipelines draaien,
    met een **eigen ingest-API-key per cluster**.
- **Test-data**: 6 nieuwe data streams bulk-geïndexeerd over de 3 clusters (76.000
  docs) om discovery/routing/coverage te valideren.

Legenda status: **Pass** / **Pass\*** (pass met kanttekening) / **N.u.** (niet
uitgevoerd) / **Fail**.

## Test-data (ingest-fixture)

| Cluster | Data stream | Docs |
|---------|-------------|------|
| es-central | `logs-nginx-prod` | 35.000 |
| es-central | `logs-application.auditd-tst` | 15.000 |
| es-remote-a | `logs-webapp-prod` | 12.000 |
| es-remote-a | `logs-nginx.access-tst` | 12.000 |
| es-remote-b | `logs-firewall-prod` | 12.000 |
| es-remote-b | `metrics-system-prod` | 10.000 |

Geïndexeerd via `scripts/simulate_datastream_load.py` / `simulate_nginx_load.py` /
`simulate_auditd_load.py` (0 index-errors). N.B. deze scripts bulk-indexeren
**direct in ES data streams**; ze produceren niet naar Kafka.

## Resultaten

| ID | Prio | Scenario (kort) | Werkelijk resultaat | Status |
|----|------|-----------------|---------------------|--------|
| INFRA-01 | Hoog | Clusters gezond | central/remote-a/remote-b alle `yellow` (1 node elk); active_shards 126/8/8 | Pass |
| INFRA-02 | Hoog | CCS/monitoring bereikbaar | `monitoring:`-CCS connected (255.139 docs). **remote_a/remote_b data-CCS = disconnected** (zelfde als vorige ronde) | Pass\* |
| INFRA-03 | Hoog | Monitoring actueel | 6.403 samples < 5 min | Pass |
| INFRA-04 | Hoog | ML-license geldig | type `trial`, status `active`, expiry **2026-07-18** (morgen!) | Pass\* |
| INFRA-05 | Hoog | ML-jobs/datafeeds | 5/5 jobs `opened`, 5/5 datafeeds `started` | Pass |
| INFRA-06 | Midden | es-central geheugen | running, restarts=0 (geen OOM), **1.78/2 GiB = 89%** | Pass\* |
| DEP-01 | Hoog | Role rolt watchers uit | `site.yml` failed=0; **8** watchers actief (registry-sync, scoring, routing-advisor, state-manager, pipeline-manager, stream-coverage, register-sync, forecast-trigger) | Pass |
| DEP-02 | Hoog | cpm-indices + templates | 12 `cpm-*` config-indices aanwezig; pipeline-templates count = **2** | Pass |
| DEP-03 | Midden | Idempotent | 2e `bootstrap.yml` → state 315→315, 4 pipelines gelijk (`changed=0`) | Pass |
| FUNC-RS-01 | Hoog | register-sync → registry | 3 docs (central-cluster/remote-a/remote-b); `dc` = dc-central/dc-a/dc-b; `ingest_hosts` = `https://es-<cluster>` (zonder poort); `active:true` | Pass |
| FUNC-FT-01 | Hoog | forecast-trigger / ML | 5 ML-jobs `opened` + datafeeds `started` (forecast-basis draait) | Pass\* |
| FUNC-SC-01 | Hoog | scoring | 1 scores-doc, **3** nested clusters; totalen 12.72 / 17.07 / 12.76 | Pass |
| FUNC-SC-02 | Laag | alert bij >80 | geen cluster >80 → `alert:false` (verwacht) | Pass |
| FUNC-RA-01 | Hoog | routing-suggesties | 1 suggestie (`logs-nginx-prod`, zwaarste stream 35k) → dedicated op lichter cluster (dc-a) | Pass |
| FUNC-SM-01 | Hoog | dedicated uit suggesties | 1 dedicated-entry: `dc-a_cpm-dedicated-<qwn>` voor `logs-nginx-prod` | Pass |
| FUNC-SM-02 | Hoog | catchall-discovery | 314 catchall-entries; alle 6 nieuwe test-streams ontdekt en toegewezen | Pass |
| FUNC-SM-03 | Midden | dedup/uniciteit | 315 state-entries, 315 unieke topics in pipelines, **0 topics in >1 pipeline** | Pass |
| FUNC-PM-01 | Hoog | Logstash-pipelines gerenderd | 4 pipelines; **0 onvervangen `__TOKEN__`**; hosts/topics/api_key correct ingevuld | Pass |
| FUNC-PM-02 | Midden | catchall gebundeld per cluster | 3 catchall (dc-central 116 / dc-a 99 / dc-b topics), één pipeline per cluster met volledige topic-lijst | Pass |
| E2E-01 | Hoog | Volledige keten | registry(3) → scores(3) → suggestie(1) → state(315) → pipelines(4) consistent; `failed=0` | Pass |
| E2E-02 | Midden | 2e ronde stabiel | state 315→315, pipelines 4→4 (idempotent) | Pass |
| SCALE-01 | Hoog | Geen cluster-truncatie | monitoring=3, registry=3, scores=3 (size-limieten 10/20 ≫ 3) | Pass |
| SCALE-02 | Hoog | Geen datastream-truncatie | 315 state-entries dekken alle ontdekte streams incl. de 6 nieuwe (composite 1000 ≫) | Pass |
| NEW-LS-01 | Hoog | Per-cluster Logstash draait eigen pipelines | logstash-central=1, remote-a=2, remote-b=1 pipeline(s) — exact hun `dc-*`-filter | Pass |
| NEW-LS-02 | Hoog | Pipelines compileren zonder fouten | 0 undefined-var, 0 compile-fouten, 0 reload-failures, 0 ES-auth-401 (per-cluster api_key) | Pass |
| NEW-CA-01 | Hoog | Topic op precies 1 cluster | 0 topics in meer dan één pipeline (elke topic uniek toegewezen) | Pass |
| NEW-CA-02 | Hoog | Catchall dekt alle non-dedicated topics | alle 6 test-streams gedekt: nginx-prod→dedicated(dc-a), auditd-tst→catchall(dc-central), webapp/nginx.access→catchall(dc-a), firewall/system→catchall(dc-b) | Pass |
| NEW-DATA-01 | Hoog | Test-data geïndexeerd | 76.000 docs over 6 data streams op 3 clusters, 0 errors | Pass |
| ENV-01 | Midden | Coexistence | 8 `cpm-*` watchers actief; geen `cpmw-*` workflows draaiend (schoon) | Pass |
| ENV-04 | Hoog | License-verval bewaakt | trial verloopt **2026-07-18T09:11Z** | Pass\* |
| E2E-KAFKA | Hoog | Data-flow Kafka→Logstash→ES | **N.u.** — geen Kafka-producer in de lokale setup; pipelines draaien idle (0 events). Test-data is direct in ES geïndexeerd | N.u. |
| NEG-01/02 | Midden | Cluster/ML offline | **N.u.** (disruptief, niet uitgevoerd) | N.u. |

## Belangrijkste bevindingen / opvolgpunten

1. **Trial-license verloopt 2026-07-18 (morgen).** Zonder geldige license stoppen
   ML/forecast/scoring/routing. Vernieuw of verleng vóór verloop. *(INFRA-04 / ENV-04)*
2. **es-central geheugen 89%** (1.78/2 GiB). Geen OOM (restarts=0), maar krap.
   Overweeg heap/limit-verhoging richting productie-schaal. *(INFRA-06)*
3. **remote_a/remote_b data-CCS = disconnected** terwijl `monitoring:`-CCS wél
   verbonden is. CPM werkt (via monitoring-CCS); controleer of de data-tier-CCS
   bewust uit staat. *(INFRA-02)*
4. **Kafka→Logstash→ES niet end-to-end getest.** De per-cluster Logstash-pipelines
   compileren en draaien correct (0 fouten, juiste api_key per cluster), maar staan
   **idle**: er is geen Kafka-producer in de lokale demo. Test-data is direct in ES
   geïndexeerd. Voor een echte data-flow-test is een producer naar de Kafka-topics
   nodig (bv. `logstash-router` of filebeat/beats → `logstash-beats`). *(E2E-KAFKA)*
5. **Routing werkt aantoonbaar:** de zwaarste stream `logs-nginx-prod` (35k docs)
   kreeg een **dedicated** pipeline toegewezen op het lichtere cluster (dc-a), de
   overige streams belanden in de per-cluster catchall. *(FUNC-RA-01/SM-01)*
6. **Catchall-bundeling correct:** elke non-dedicated topic zit in precies één
   gebundelde catchall-pipeline op precies één cluster; volledige dekking, geen
   duplicaten (opgelost t.o.v. de eerdere per-dataset/duplicatie-problemen).

## Niet getest in deze omgeving

- **E2E-KAFKA** — echte Kafka→Logstash→ES data-flow (geen producer lokaal).
- **NEG-01/02** (cluster offline / ML-fail) — disruptief, niet uitgevoerd.
- **Échte 7-cluster / 44-datastream schaal** — alleen op de doelomgeving te valideren.
- **`logstash-router`** — staat in de compose maar is niet gestart (aparte
  beats→Kafka-routingfunctie; config lokaal niet aanwezig).

---
*Uitvoerder: Claude (geautomatiseerd). Branch `mod-imre3006`, stack 9.4.2.
Zie `docs/dagboek/2026-07-17.md` voor de volledige stappen van deze sessie.*

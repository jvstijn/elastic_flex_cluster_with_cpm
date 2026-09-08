# Testresultaten — CPM watchers + per-cluster Logstash (2026-08-11)

Herstart van de lokale stack (`docker-local/`) na ~3 weken stilstand, gevolgd door
een volledige testronde volgens `docker-local/TESTPLAN.md`, aangepast aan de
huidige stand op branch `mod-jvs_0717`:

- **3 clusters** (central + remote-a + remote-b), stack **9.4.2**.
- Systeem onder test = de **`cpm-*` watchers** (uitgerold via `ansible-playbook
  site.yml` + `bootstrap.yml`), **niet** de `cpmw-*` workflows.
- **Nieuw t.o.v. 2026-07-17**: de keten Kafka → Logstash → Elasticsearch is deze
  ronde **wél end-to-end getest** (vorige ronde stond dit open), inclusief de
  `logstash-router` (routing + dead-letter-queue) en het **dedicated**-pipelinepad.

Legenda status: **Pass** / **Pass\*** (pass met kanttekening) / **N.u.** (niet
uitgevoerd) / **Fail**.

## Bevindingen die tijdens deze ronde zijn opgelost

| # | Bevinding | Impact | Oplossing |
|---|-----------|--------|-----------|
| 1 | `logstash-router`-config ontbrak in `docker-local/config/` (stond alleen in root `config/`) | `docker compose up` faalde: mount-fout, hele stack kwam niet omhoog | Config gekopieerd naar `docker-local/config/logstash-router/` (`logstash.yml`, `valid_topics.yml`, `pipeline/router.conf`) |
| 2 | Registry: alle clusters `active: false` + 3 stale entries (uuid's van vóór de rebuild) | `cpm-scoring` gaf 0 clusters, `cpm-state-manager` ontdekte 0 datastreams, **0 pipelines** — dit was het "openstaande punt" uit het dagboek van 17-07 | Stale docs verwijderd, 3 live clusters op `active: true` gezet (operator-actie, wordt niet door de playbook gezet) |
| 3 | Pipeline-templates: ES-output installeert `ecs-logstash` index-template met de **ingest-API-key** → HTTP 403 → *"Failed to bootstrap. Pipeline is going to shut down"* | **Alle** CPM-pipelines vielen direct na start om; geen enkele ingest mogelijk | `manage_template => false` toegevoegd in de `filebeat`-tak van beide templates |
| 4 | Dedicated-template: `index => "filebeat-…"` stond samen met `data_stream => true` in dezelfde output | Dedicated pipelines konden **nooit** starten (`LogStash::ConfigurationError: Invalid data stream configuration: ["index"]`, apart gereproduceerd) | De `index`-regel verwijderd uit de data_stream-tak |
| 5 | CPM Kibana-plugin was verdwenen na het opnieuw aanmaken van de kibana-container | `/app/cpm` en de CPM-API's niet beschikbaar | `cpm-9.4.2.zip` uit het build-image opnieuw geïnstalleerd (`cpm@1.0.6`) + restart |

Bevindingen 3 en 4 zitten in **gedeelde** template-bestanden
(`ansible/roles/elastic_cpm/files/json/cpm-pipeline-template-{catchall,dedicated}.json`)
en raken dus ook kaposi/productie — daar gelden dezelfde twee fouten.

## Test-data (ingest-fixture)

Bestaande fixture uit de vorige ronde (76.000 docs), aangevuld met 25 events die
via **Kafka** door de CPM-pipelines zijn gestuurd.

| Cluster | Data stream | Docs |
|---------|-------------|------|
| es-central | `logs-nginx-prod` | 15.005 (15.000 + 5 via router) |
| es-central | `logs-application.auditd-tst` | 15.000 |
| es-remote-a | `logs-webapp-prod` | 12.020 (12.000 + 10 catchall + 10 dedicated) |
| es-remote-a | `logs-nginx.access-tst` | 12.000 |
| es-remote-b | `logs-firewall-prod` | 12.000 |
| es-remote-b | `metrics-system-prod` | 10.000 |

## Resultaten

| ID | Prio | Scenario (kort) | Werkelijk resultaat | Status |
|----|------|-----------------|---------------------|--------|
| INFRA-01 | Hoog | Clusters gezond | central/remote-a/remote-b alle `yellow` (1 node elk); active_shards 76/5/5, 0 restarts | Pass |
| INFRA-02 | Hoog | CCS/monitoring bereikbaar | `monitoring:`-CCS `connected` (375.263 docs). **remote_a/remote_b data-CCS = `connected:false`** (seeds `:9443`, ongewijzigd t.o.v. vorige rondes) | Pass\* |
| INFRA-03 | Hoog | Monitoring actueel | 4.097 samples < 5 min, alle 3 clusters | Pass |
| INFRA-04 | Hoog | ML-license geldig | type `trial`, status `active`, expiry **2026-08-16** (nog 5 dagen) | Pass\* |
| INFRA-05 | Hoog | ML-jobs/datafeeds | 5/5 jobs `opened`, 5/5 datafeeds `started` | Pass |
| INFRA-06 | Midden | Geheugen / geen OOM | 0 restarts; es-central 1,77/2 GiB (88%), **es-remote-a 99,8% en es-remote-b 99,7% van 1 GiB** | Pass\* |
| DEP-01 | Hoog | Role rolt watchers uit | `site.yml` `failed=0`, `changed=0` (idempotent); **7** watchers actief | Pass |
| DEP-02 | Hoog | cpm-indices + templates | 12 `cpm-*` indices; pipeline-templates count = **2** | Pass |
| DEP-03 | Midden | Idempotent | 2e + 3e `bootstrap.yml` → state 7→7, pipelines 3→3, `changed=0` | Pass |
| FUNC-RS-01 | Hoog | register-sync → registry | 3 docs; `dc` = dc-central/dc-a/dc-b; `ingest_hosts` per cluster; `disk_total_bytes`/`heap_max_bytes`/`node_count` gevuld | Pass |
| FUNC-RS-02 | Midden | Behoud velden bij her-run | `active`/`dc`/`cluster_name` blijven behouden over meerdere runs | Pass |
| FUNC-FT-01 | Hoog | forecast-trigger start forecasts | 4 acties `success`, elk een forecast; `forecasts_stats.total` 67→68 per job | Pass |
| FUNC-SC-01 | Hoog | scoring berekent score per cluster | 3 nested clusters; totalen 20,04 (central) / 19,22 (remote-a) / 19,27 (remote-b) met echte forecastwaarden | Pass |
| FUNC-SC-02 | Laag | alert bij >80 | geen cluster >80 → `alert:false` | Pass |
| FUNC-RA-01 | Hoog | routing-suggesties bij load | **N.u.** — te weinig verschil in event rates na herstart | N.u. |
| FUNC-RA-02 | Midden | routing-advisor zonder rates | watcher `executed`, actie `condition_failed`, 0 suggesties, **geen fout** | Pass |
| FUNC-SM-01 | Hoog | dedicated entries | Handmatig geforceerde dedicated state-entry → correct gerenderde dedicated pipeline (zie E2E-04) | Pass\* |
| FUNC-SM-02 | Hoog | catchall-discovery | Alle 6 datastreams ontdekt en toegewezen aan het eigen cluster; 7 state-entries (incl. `logs-beats-raw`) | Pass |
| FUNC-SM-03 | Midden | dedup/uniciteit | 7 unieke topics, **0 topics in >1 pipeline**; her-run geeft geen duplicaten | Pass |
| FUNC-PM-01 | Hoog | Logstash-pipelines gerenderd | 3 pipelines; **0 onvervangen `__TOKEN__`**; `hosts`/`topics`/`group_id`/`api_key`/`pipeline_settings` correct | Pass |
| FUNC-PM-02 | Midden | catchall gebundeld per cluster | dc-central 3 topics, dc-a 2, dc-b 2 — één pipeline per cluster met volledige topiclijst | Pass |
| E2E-01 | Hoog | Volledige keten | registry(3) → scores(3) → suggesties(0) → state(7) → pipelines(3) consistent; `failed=0` | Pass |
| E2E-02 | Midden | 2e ronde stabiel | state 7→7, pipelines 3→3 (idempotent) | Pass |
| **E2E-03** | **Hoog** | **Kafka → Logstash → ES (nieuw)** | 10 events naar topic `logs-webapp-prod` → binnen ~10 s in `.ds-logs-webapp-prod` op **es-remote-a** (12.000→12.010); doc bevat `event.lag.31.label` = pipeline-id en hostname-tag | **Pass** |
| **E2E-04** | **Hoog** | **Dedicated pipeline (nieuw)** | Geforceerde dedicated state-entry → pipeline gerenderd met `data_stream_dataset => "webapp"`, opgepakt door logstash-remote-a, 10 events geïngest (12.010→12.020), 0 fouten | **Pass** |
| **E2E-05** | **Hoog** | **logstash-router (nieuw)** | 5 events met bekend `data_stream` → topic `logs-nginx-prod` → es-central (15.000→15.005); 3 events met onbekend stream → topic `dead-letter-queue` met `dlq.reason` "target topic does not exist" en `dlq.intended_topic` | **Pass** |
| LS-01 | Hoog | Per-cluster Logstash draait eigen pipelines | central 1, remote-a 1, remote-b 1 pipeline (eigen `dc-*`-filter); **0 reload-failures, 0 compile-fouten, 0 401's** ná fix #3 | Pass |
| COV-01 | Midden | stream-coverage gevuld | 2 docs — alleen de streams met **recente** ingest (`logs-nginx-prod`, `logs-webapp-prod`); leeg zolang er niets ingest | Pass\* |
| KBN-01 | Midden | Kibana + CPM-plugin | Kibana `available` (9.4.2); plugin `cpm@1.0.6` herinstalleerd; `/api/cpm/clusters` geeft de 3 actieve clusters | Pass |
| KBN-02 | Laag | CPM-dashboards | 4 dashboards uitgerold (platform-overview, pipeline-assignments, pipeline-history, stream-locks) | Pass |

## Openstaande punten / risico's

1. **`active: true` is een handmatige stap.** Noch `site.yml` noch `bootstrap.yml`
   zet clusters actief; `cpm-registry-sync` schrijft `active: false` voor nieuwe
   clusters en behoudt daarna de bestaande waarde. Na een verse installatie doet
   de hele keten daardoor niets, zonder foutmelding. Overweeg `active` op te nemen
   in `cpm_cluster_registry` (group_vars) of in de bootstrap-patch.
2. **Geheugen op de remotes**: es-remote-a/-b zitten op ~99,7% van hun 1 GiB-limiet.
   Eerdere containers zijn met `Exited (137)` (OOM) gestopt. Verhoog `mem_limit`
   of verlaag de heap.
3. **Trial-license verloopt 2026-08-16** — daarna vallen ML-forecasts (en dus
   scoring/routing) weg.
4. **data-CCS naar remote_a/remote_b blijft `connected:false`** (seeds op poort
   `:9443`). Alleen de `monitoring`-remote werkt; de keten gebruikt die, dus
   functioneel geen blokkade.
5. **`valid_topics.yml` van de router is verouderd** — het is de kaposi-lijst (313
   topics) en bevat de lokale streams grotendeels niet (`logs-webapp-prod`
   ontbreekt, `logs-nginx-prod` staat er wel in). Events voor onbekende streams
   gaan correct naar de DLQ, maar voor een realistische routertest moet de
   dictionary opnieuw gegenereerd worden.
6. **`FUNC-RA-01` (routing-suggesties onder load) is niet uitgevoerd** — daarvoor
   is langdurige, ongelijk verdeelde load over de clusters nodig.
7. **CPM-plugin zit niet in de compose/image**: hij wordt handmatig in de draaiende
   kibana-container geïnstalleerd en is dus weg zodra de container opnieuw wordt
   aangemaakt.

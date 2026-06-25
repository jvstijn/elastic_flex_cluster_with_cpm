# Testplan — CPM Workflows (air-gapped, 7 clusters, 44 datastreams)

Testplan om te valideren dat het CPM-systeem (de 6 `cpmw-*` Elastic 9.4 Workflows
en de onderliggende keten) correct werkt in een air-gapped omgeving met
**7 clusters** en **44 datastreams**.

> Vul tijdens uitvoering per regel **Werkelijk resultaat** en **Status**
> (`Open` → `Pass` / `Fail` / `N.v.t.`) in.

## Conventies

```bash
ES=https://localhost:9200            # es-central
KB=http://localhost:5601             # Kibana
PW='<elastic-wachtwoord>'            # = ELASTIC_PASSWORD uit .env
EC="curl -sk -u elastic:$PW"                         # ES (self-signed TLS)
KC="curl -s -u elastic:$PW -H kbn-xsrf:true"         # Kibana API

# Workflow op naam draaien + status tonen (ids hebben in Kibana een -N suffix):
run_wf() { # bijv: run_wf cpmw-scoring
  id=$($KC "$KB/api/workflows?size=1000" | python3 -c "import sys,json,os;print(next(w['id'] for w in json.load(sys.stdin)['results'] if w['name']==os.environ['N']))" N="$1")
  ex=$($KC -XPOST "$KB/api/workflows/workflow/$id/run" -H 'Content-Type: application/json' -d '{"inputs":{}}' | python3 -c "import sys,json;print(json.load(sys.stdin)['workflowExecutionId'])")
  sleep 10
  $KC "$KB/api/workflows/executions/$ex" | python3 -c "import sys,json;d=json.load(sys.stdin);print('STATUS',d['status']);[print(' ',s['stepId'],s['status'],(s.get('error') or '')) for s in d['stepExecutions']]"
}
```

Workflows kunnen ook via **Kibana → Stack Management → Workflows** (Run) gedraaid
worden. Volgorde van de keten: `register-sync → forecast-trigger → (wacht op ML) →
scoring → routing-advisor → state-manager → pipeline-manager`.

## Algemene precondities (eenmalig, vóór de functionele tests)

- Stack volledig up; alle 7 ES-clusters bereikbaar en status `green`/`yellow`.
- Cross-cluster search (`monitoring:.monitoring-es-8-*`) werkt vanaf es-central.
- Metricbeat-monitoring is actueel (data van < 5 min oud).
- Geldige ML-license (Trial/Platinum) — **let op: een self-generated Trial verloopt
  na 30 dagen; in air-gapped kan dat niet online vernieuwd worden** (zie INFRA-04).
- `elastic_cpm` rol toegepast (ML-jobs + datafeeds bestaan); `elastic_cpm_workflows`
  rol toegepast (`ansible-playbook workflows.yml`).

---

## 1. Infrastructuur / precondities

| ID | Testtype | Prio | Testscenario | Preconditie | Teststappen | Verwacht resultaat | Werkelijk resultaat | Status |
|----|----------|------|--------------|-------------|-------------|--------------------|---------------------|--------|
| INFRA-01 | Infra | Hoog | Alle 7 clusters bereikbaar en gezond | Stack up | 1. Per cluster: `$EC <cluster-url>/_cluster/health` | 7× status `green`/`yellow`, geen `red`; `number_of_nodes` ≥ verwacht | | Open |
| INFRA-02 | Infra | Hoog | CCS/monitoring cross-cluster bereikbaar vanaf central | Remote clusters geconfigureerd | 1. `$EC "$ES/_remote/info"` <br>2. `$EC "$ES/monitoring:.monitoring-es-8-*/_count"` | Alle remotes `connected:true`; count > 0 | | Open |
| INFRA-03 | Infra | Hoog | Monitoring-data actueel (cluster/node/index) | Metricbeat draait | 1. `$EC "$ES/.monitoring-es-8-*/_search" -d '{"size":0,"query":{"range":{"@timestamp":{"gte":"now-5m"}}}}'` (header Content-Type:application/json) | `hits.total` > 0 (verse samples van alle 7 clusters) | | Open |
| INFRA-04 | Infra | Hoog | ML-license geldig (Trial/Platinum, niet verlopen) | — | 1. `$EC "$ES/_license"` <br>2. `$EC "$ES/_xpack" \| grep -i ml` | `license.status:active`, type Trial/Platinum; `expiry_date` in de toekomst; ML `available:true,enabled:true` | | Open |
| INFRA-05 | Infra | Hoog | Alle cpm ML-jobs `opened` en datafeeds `started` | ML-jobs aangemaakt | 1. `$EC "$ES/_ml/anomaly_detectors/cpm-*/_stats?h=id,state"` <br>2. `$EC "$ES/_ml/datafeeds/datafeed-cpm-*/_stats"` | 5 jobs `opened`, 5 datafeeds `started` | | Open |
| INFRA-06 | Infra/Res | Midden | es-central blijft binnen geheugenlimiet (geen OOM) | Memory-fix toegepast (heap 1g / limit 2g) | 1. `docker stats --no-stream dod-elastic-es-central-1` <br>2. `docker inspect -f '{{.State.Status}} {{.RestartCount}}' dod-elastic-es-central-1` | MemPerc < ~90%; geen `Exited (137)`; RestartCount stabiel | | Open |

## 2. Deployment

| ID | Testtype | Prio | Testscenario | Preconditie | Teststappen | Verwacht resultaat | Werkelijk resultaat | Status |
|----|----------|------|--------------|-------------|-------------|--------------------|---------------------|--------|
| DEP-01 | Deploy | Hoog | Playbook rolt 6 workflows uit, allen valid | Kibana bereikbaar | 1. `source <venv> && ansible-playbook workflows.yml` | `failed=0`; 6 regels `valid=True` (forecast-trigger, register-sync, scoring, routing-advisor, state-manager, pipeline-manager) | | Open |
| DEP-02 | Deploy | Hoog | cpmw-indices + pipeline-templates aangemaakt | DEP-01 | 1. `$EC "$ES/cpmw-cluster-registry,cpmw-scores/_mapping"` <br>2. `$EC "$ES/cpmw-pipeline-templates/_count"` | Beide indices bestaan met strict mapping; templates count = 2 | | Open |
| DEP-03 | Deploy/Idem | Midden | Idempotente her-uitrol → exact 6 workflows | DEP-01 | 1. `ansible-playbook workflows.yml` (2e keer) <br>2. `$KC "$KB/api/workflows?size=1000"` \| tel cpmw-* | `failed=0`; precies 6 cpmw-* workflows, één per naam (geen duplicaten) | | Open |

## 3. Functioneel per workflow

| ID | Testtype | Prio | Testscenario | Preconditie | Teststappen | Verwacht resultaat | Werkelijk resultaat | Status |
|----|----------|------|--------------|-------------|-------------|--------------------|---------------------|--------|
| FUNC-RS-01 | Functioneel | Hoog | register-sync vult registry met alle 7 clusters | INFRA-02/03 | 1. `run_wf cpmw-register-sync` <br>2. `$EC "$ES/cpmw-cluster-registry/_search?size=20"` | Alle steps `completed`; **7** docs; per cluster correcte `disk_total_bytes` (~85% som node-disk), `heap_max_bytes`, `node_count`, `shard_max_threshold` (= heap/1GiB×20), niet-lege `ingest_hosts`, `active:true` | | Open |
| FUNC-RS-02 | Functioneel | Midden | register-sync behoudt bestaande velden bij her-run | FUNC-RS-01 | 1. Pas `dc`/`cluster_name` handmatig aan op 1 doc <br>2. `run_wf cpmw-register-sync` <br>3. Bekijk dat doc | `cluster_id`, `cluster_name`, `active`, (en eventueel `dc`) blijven behouden; capaciteitsvelden ververst | | Open |
| FUNC-FT-01 | Functioneel | Hoog | forecast-trigger start ML-forecasts | INFRA-05 | 1. `run_wf cpmw-forecast-trigger` <br>2. `$EC "$ES/_ml/anomaly_detectors/cpm-store-size/_stats" \| grep forecast` | 4 steps `completed`, elk een `forecast_id`; `forecasts_stats.total` neemt toe | | Open |
| FUNC-SC-01 | Functioneel | Hoog | scoring berekent gewogen score per cluster | FUNC-RS-01, ML-forecasts aanwezig | 1. `run_wf cpmw-scoring` <br>2. `$EC "$ES/cpmw-scores/_search?sort=scored_at:desc&size=1"` | 1 nieuw doc; `clusters` bevat **7** entries; `total_score` ≈ 0.5·disk + 0.25·jvm + 0.05·shard + 0.2·load (spot-check 1 cluster) | | Open |
| FUNC-SC-02 | Functioneel | Laag | scoring zet alert bij total_score > 80 | FUNC-SC-01 | 1. Forceer hoge forecast óf inspecteer een zwaar cluster <br>2. Bekijk `alert` in scores | `alert:true` voor clusters met `total_score > 80`, anders `false` | | Open |
| FUNC-RA-01 | Functioneel | Hoog | routing-advisor stelt verplaatsingen voor bij load | Load actief op datastreams; ≥2 uur monitoring-historie | 1. `run_wf cpmw-routing-advisor` <br>2. `$EC "$ES/cpmw-routing-suggestions/_search?size=50"` | Index bestaat; suggesties met `reason` "Move … from <zwaar> to <licht>"; `suggested_cluster_id` = lichter cluster; `event_rate_1h` > 0 | | Open |
| FUNC-RA-02 | Functioneel/Neg | Midden | routing-advisor zonder rates → geen suggesties, geen fout | Geen recente datastream-ingest | 1. `run_wf cpmw-routing-advisor` | Status `completed`; `suggestions:0`; `foreach` draait 0×; **geen** fout, geen lege-_bulk-error | | Open |
| FUNC-SM-01 | Functioneel | Hoog | state-manager maakt dedicated entries uit suggesties | FUNC-RA-01 | 1. `run_wf cpmw-state-manager` <br>2. `$EC "$ES/cpmw-pipeline-state/_search?q=pipeline_type:dedicated&size=50"` | Per gesuggereerd cluster 1 dedicated entry; `pipeline_id = <dc>_cpm-dedicated-<cluster>`; `topic = <type>-<dataset>-<ns>` | | Open |
| FUNC-SM-02 | Functioneel | Hoog | state-manager ontdekt catchall-datastreams uit monitoring | 44 datastreams actief | 1. `run_wf cpmw-state-manager` <br>2. `$EC "$ES/cpmw-pipeline-state/_count"` | Catchall entries voor niet-dedicated datastreams; totaal aantal state-entries dekt alle 44 datastreams (dedicated + catchall) | | Open |
| FUNC-SM-03 | Functioneel | Midden | state-manager dedup + managed-topic-exclusie | FUNC-RA-01 + bestaande state | 1. `run_wf cpmw-state-manager` (2×) <br>2. Inspecteer state | Per `_id` (type-dataset-ns) max 1 entry; een dedicated topic verschijnt niet óók als catchall; her-run geeft geen duplicaten | | Open |
| FUNC-PM-01 | Functioneel | Hoog | pipeline-manager pusht gerenderde Logstash-pipelines | FUNC-SM-01/02 | 1. `run_wf cpmw-pipeline-manager` <br>2. `$EC "$ES/_logstash/pipeline"` <br>3. Inspecteer 1 pipeline-config | Pipelines aangemaakt per `pipeline_id`; **geen** `__TOKEN__` over; `topics`, `hosts => ["<host>:9200"]`, `group_id => "cpm-<naam>"`, `api_key => "${ES_API_KEY_<ID>}"` correct; `pipeline_settings` aanwezig | | Open |
| FUNC-PM-02 | Functioneel | Midden | pipeline-manager bundelt catchall-topics per cluster | Catchall state-entries | 1. Bekijk een catchall-pipeline-config | `topics => [...]` bevat alle (ontdubbelde) catchall-topics van dat cluster; `description` "… (N datasets)" klopt | | Open |

## 4. End-to-end keten

| ID | Testtype | Prio | Testscenario | Preconditie | Teststappen | Verwacht resultaat | Werkelijk resultaat | Status |
|----|----------|------|--------------|-------------|-------------|--------------------|---------------------|--------|
| E2E-01 | E2E | Hoog | Volledige keten op 7 clusters / 44 datastreams | Load actief; ≥2 uur monitoring | 1. `run_wf cpmw-register-sync` <br>2. `run_wf cpmw-forecast-trigger`; wacht tot forecasts klaar <br>3. `run_wf cpmw-scoring` <br>4. `run_wf cpmw-routing-advisor` <br>5. `run_wf cpmw-state-manager` <br>6. `run_wf cpmw-pipeline-manager` | Registry 7, scores 7-nested, suggesties > 0, state dekt 44 datastreams, Logstash-pipelines weerspiegelen de toewijzing; alle workflows `completed` zonder fouten | | Open |
| E2E-02 | E2E | Midden | Tweede volledige ronde is stabiel (convergentie) | E2E-01 | 1. Herhaal E2E-01 zonder load-wijziging | Geen nieuwe/verdwenen pipelines t.o.v. ronde 1; state-entries identiek (idempotent) | | Open |

## 5. Schaal (7 clusters / 44 datastreams)

| ID | Testtype | Prio | Testscenario | Preconditie | Teststappen | Verwacht resultaat | Werkelijk resultaat | Status |
|----|----------|------|--------------|-------------|-------------|--------------------|---------------------|--------|
| SCALE-01 | Schaal | Hoog | Geen truncatie van clusters (size-limieten) | 7 clusters actief | 1. Vergelijk #clusters in monitoring vs `cpmw-cluster-registry` vs `cpmw-scores.clusters` vs routing `by_cluster` buckets | Overal **7** (geen afkapping). N.B. scoring/routing aggregaties gebruiken `size:10` → veilig bij 7, herzie bij > 10 clusters | | Open |
| SCALE-02 | Schaal | Hoog | Alle 44 datastreams ontdekt (geen truncatie) | 44 datastreams | 1. Tel unieke datastreams in monitoring (`.ds-logs/metrics/traces-*`) <br>2. Tel state-entries + dedicated/catchall topics | Alle 44 vertegenwoordigd in `cpmw-pipeline-state`. N.B. discovery composite `size:1000`, `existing_state size:500` → veilig bij 44 | | Open |
| SCALE-03 | Schaal | Midden | routing-advisor verwerkt top-N over 7 clusters | Load op meerdere clusters | 1. `run_wf cpmw-routing-advisor`; inspecteer suggesties | Max 1 doel per cluster (greedy); top-N streams = aantal clusters; swaps/lokaal-dedicated logica consistent | | Open |

## 6. Negatief / resilience

| ID | Testtype | Prio | Testscenario | Preconditie | Teststappen | Verwacht resultaat | Werkelijk resultaat | Status |
|----|----------|------|--------------|-------------|-------------|--------------------|---------------------|--------|
| NEG-01 | Negatief | Midden | Cluster offline tijdens register-sync | 1 remote tijdelijk gestopt | 1. Stop 1 remote <br>2. `run_wf cpmw-register-sync` <br>3. Inspecteer registry | Workflow `completed` (geen crash); offline cluster verschijnt niet in nieuwe monitoring-buckets maar bestaand registry-doc blijft behouden | | Open |
| NEG-02 | Negatief | Midden | ML-job `failed` na ES-herstart | Forceer herstart van een ES-node | 1. Herstart node; check job-state <br>2. `run_wf cpmw-forecast-trigger` / `cpmw-scoring` | Bij `failed` job: forecast/scoring degraderen netjes (score 0 voor die dimensie, geen crash). Herstel: `_close?force=true` → reopen → datafeed start | | Open |
| NEG-03 | Negatief | Midden | < 2 monitoring-buckets → rate 0 | Verse datastream (< 1 uur) | 1. Nieuwe datastream, kort load <br>2. `run_wf cpmw-routing-advisor` | Geen suggestie voor die stream (delta vereist ≥2 uurlijkse buckets); geen fout. Suggesties komen pas na voldoende historie + continue ingest | | Open |
| NEG-04 | Idempotentie | Midden | Her-uitvoer geeft geen duplicaten | Eén volledige ronde gedaan | 1. Draai elke workflow 2× <br>2. Tel docs in elke cpmw-index | Geen duplicaten (writes zijn upserts op `_id`); aantallen stabiel | | Open |

## 7. Omgeving / coexistence / air-gapped

| ID | Testtype | Prio | Testscenario | Preconditie | Teststappen | Verwacht resultaat | Werkelijk resultaat | Status |
|----|----------|------|--------------|-------------|-------------|--------------------|---------------------|--------|
| ENV-01 | Coexistence | Midden | Watchers en workflows storen elkaar niet | Beide actief | 1. `$EC "$ES/_watcher/stats"` <br>2. Draai cpmw-keten <br>3. Vergelijk `cpm-*` vs `cpmw-*` indices | 6 watchers actief; workflows schrijven uitsluitend naar `cpmw-*`; `cpm-*` indices onveranderd door workflows (en omgekeerd) | | Open |
| ENV-02 | Air-gapped | Hoog | Geen uitgaande netwerkcalls nodig | — | 1. Inspecteer dat workflows alleen `elasticsearch.request` (intern) gebruiken <br>2. Monitor host-netwerk tijdens een ronde | Geen externe verbindingen; alles via interne ES/Kibana; ML/forecast/registry werken zonder internet | | Open |
| ENV-03 | Air-gapped | Midden | Uitrol werkt zonder internet | Images + venv lokaal aanwezig | 1. `docker compose up -d` (offline) <br>2. `ansible-playbook workflows.yml` (offline) | Stack start; geen image-pulls/registry-calls; playbook slaagt volledig offline | | Open |
| ENV-04 | Air-gapped | Hoog | License-vervaldatum bewaakt (Trial 30d) | — | 1. `$EC "$ES/_license" \| grep expiry` <br>2. Noteer datum + alert/herinnering | `expiry_date` bekend en bewaakt; vóór verloop een geldige license toegepast, anders stopt ML (en daarmee forecast/scoring) | | Open |

---

## Bekende aandachtspunten (lees vóór testen)

1. **routing-advisor heeft historie + continue load nodig.** De rate wordt berekend
   als groei van `index_total` over ≥2 uurlijkse monitoring-buckets binnen `now-2h`.
   Een eenmalige burst of een verse datastream geeft (nog) geen suggesties. Voor
   stabiele suggesties moet load **continu** lopen.
2. **Size-limieten in de workflows** (hard-coded): registry/scores aggregaties
   `size:10`/`20`, discovery `by_index size:500`, composite `size:1000`,
   state/pipeline searches `size:500`. Bij **7 clusters / 44 datastreams ruim
   voldoende**, maar verhoog ze vóór schaalvergroting (> 10 clusters of > ~500
   datastreams).
3. **String-getypeerde `_source`.** cpmw-docs tonen waarden als strings (Liquid);
   ES indexeert correct getypeerd, dus queries/aggregaties werken normaal.
4. **Workflow-ids krijgen een `-N` suffix** in Kibana na her-uitrol; filter/refereer
   op **naam**, niet op id.
5. **ML & license** zijn de kritische air-gapped afhankelijkheid: zonder geldige
   license stoppen de ML-jobs en daarmee forecast-trigger/scoring/routing.

## Resultatenlogboek

Noteer per testronde: datum, versie/commit, uitvoerder, en per ID de uitkomst.
Faalt een test, leg de execution-id en stap-fout vast via
`GET $KB/api/workflows/executions/{exid}` (veld `error` + `stepExecutions`).

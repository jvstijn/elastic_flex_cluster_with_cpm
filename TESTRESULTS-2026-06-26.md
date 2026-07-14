# Testresultaten — huidige setup (2026-06-26)

Uitvoering van `TESTPLAN.md` tegen de **lokale draaiende setup**: 3 clusters
(central + remote-a + remote-b) i.p.v. 7, watchers (`cpm-*`) én cpmw-workflows
live (incl. filebeat-support, branch `mod-jan`). Systeem onder test = de
**cpmw-workflows**.

Legenda status: **Pass** / **Pass\*** (pass met kanttekening) / **N.u.** (niet
uitgevoerd) / **Fail**.

| ID | Prio | Scenario (kort) | Werkelijk resultaat | Status |
|----|------|-----------------|---------------------|--------|
| INFRA-01 | Hoog | Clusters gezond | central `yellow` (1 node), remote-a/b `green` | Pass |
| INFRA-02 | Hoog | CCS/monitoring bereikbaar | `monitoring:`-CCS connected, 3.4M docs. **remote_a/remote_b data-CCS = disconnected** | Pass\* |
| INFRA-03 | Hoog | Monitoring actueel | 4297 samples < 5 min | Pass |
| INFRA-04 | Hoog | ML-license geldig | type `trial`, status `active`, expiry **2026-07-18** | Pass\* |
| INFRA-05 | Hoog | ML-jobs/datafeeds | 5/5 jobs `opened`, 5/5 datafeeds `started` | Pass |
| INFRA-06 | Midden | es-central geheugen | running, restarts=0 (geen OOM), maar **1.82/2 GiB = 91%** | Pass\* |
| DEP-01 | Hoog | 6 workflows valid | 6× `valid=True` | Pass |
| DEP-02 | Hoog | cpmw-indices+templates | indices aanwezig, 2 templates | Pass |
| DEP-03 | Midden | Idempotent → 6 | exact 6, één per naam | Pass |
| FUNC-RS-01 | Hoog | register-sync → registry | 3 docs: remote-a, remote-b, central | Pass |
| FUNC-RS-02 | Midden | Behoud velden bij re-run | niet apart geforceerd; upsert op `_id` (zie NEG-04) | N.u. |
| FUNC-FT-01 | Hoog | forecast-trigger | `completed`, 4 forecast-steps | Pass |
| FUNC-SC-01 | Hoog | scoring | scores-doc, 3 nested clusters, total≈12.7 | Pass |
| FUNC-SC-02 | Laag | alert bij >80 | geen cluster >80 → `alert:false` (verwacht) | Pass |
| FUNC-RA-01 | Hoog | routing-suggesties | 1 suggestie (`logs-nginx-prod`), index aanwezig | Pass |
| FUNC-RA-02 | Midden | leeg → geen fout | routing `completed`, geen crash | Pass |
| FUNC-SM-01 | Hoog | dedicated uit suggesties | dedicated-entry `logs-nginx-prod` | Pass |
| FUNC-SM-02 | Hoog | catchall-discovery | catchall-entry `filebeat` ontdekt | Pass |
| FUNC-SM-03 | Midden | dedup/managed-exclusie | geen duplicaten in state (zie NEG-04) | Pass |
| FUNC-PM-01 | Hoog | Logstash-pipelines | catchall(central)+dedicated(remote-a) gerenderd, tokens vervangen | Pass |
| FUNC-PM-02 | Midden | catchall topics per cluster | central catchall bevat `filebeat` + if-branch | Pass |
| E2E-01 | Hoog | Volledige keten | alle 6 `completed`; registry→scores→suggestie→state→pipelines consistent | Pass |
| E2E-02 | Midden | 2e ronde stabiel | state-count gelijk na 2e run | Pass |
| SCALE-01 | Hoog | Geen cluster-truncatie | monitoring=3, registry=3 (size-limieten 10/20 ≫ 3) | Pass |
| SCALE-02 | Hoog | Geen datastream-truncatie | alle ontdekte streams in state (composite 1000 ≫) | Pass |
| SCALE-03 | Midden | routing top-N | zinnige toewijzing (1 suggestie verwerkt) | Pass |
| NEG-01 | Midden | Cluster offline | **niet uitgevoerd** (disruptief) | N.u. |
| NEG-02 | Midden | ML-job failed | **niet uitgevoerd** (disruptief; ML-herstel na OOM eerder in sessie wél gezien) | N.u. |
| NEG-03 | Midden | <2 buckets → 0 | eerder in sessie bevestigd (rate vereist ≥2 buckets) | Pass |
| NEG-04 | Midden | Geen duplicaten | state vóór=2, ná 2e run=2 (upsert) | Pass |
| ENV-01 | Midden | Coexistence | 6 watches actief; `cpm-*` en `cpmw-*` indices gescheiden | Pass\* |
| ENV-02 | Hoog | Geen externe calls | workflows gebruiken alleen interne `elasticsearch.request` | Pass |
| ENV-03 | Midden | Offline deploy | n.v.t. in deze (online) omgeving | N.u. |
| ENV-04 | Hoog | License-verval bewaakt | trial verloopt **2026-07-18** | Pass\* |

## Belangrijkste bevindingen / opvolgpunten

1. **es-central geheugen op 91%** (1.82/2 GiB). Geen OOM (restarts=0), maar krap
   onder belasting. Overweeg heap/limit naar 1.5g/3g (vooral richting de
   7-cluster/44-datastream productie-schaal). *(INFRA-06)*
2. **remote_a/remote_b data-CCS = disconnected** terwijl `monitoring:`-CCS wél
   verbonden is. CPM werkt (gebruikt monitoring-CCS), maar controleer of de
   data-tier-CCS bewust uit staat. *(INFRA-02)*
3. **Trial-license verloopt 2026-07-18** — in air-gapped niet online te
   vernieuwen; zonder geldige license stoppen ML/forecast/scoring/routing.
   *(INFRA-04 / ENV-04)*
4. **Pipeline-id-collisie (coexistence):** cpm-watchers én cpmw-workflows
   schrijven naar dezelfde Logstash-pipeline-ids (`<dc>_cpm-(catchall|dedicated)-
   <cluster>`). Naast elkaar draaien overschrijft elkaars Logstash-pipelines.
   Indices zijn wél gescheiden (`cpm-*` vs `cpmw-*`). *(ENV-01)*
5. **Schaal niet representatief:** deze omgeving heeft 3 clusters / weinig
   datastreams. De size-limieten (10/20/500/1000) zijn ruim voldoende hier, maar
   herzie ze vóór 7 clusters / 44 datastreams (m.n. de `size:10`-aggregaties).

## Niet getest in deze omgeving
- NEG-01/02 (cluster offline / ML-fail) — disruptief; niet uitgevoerd.
- ENV-03 (offline deploy) — n.v.t. (online dev-omgeving).
- Échte 7-cluster/44-datastream schaal (SCALE) — alleen op de doelomgeving te valideren.

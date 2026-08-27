# Runbook — een data stream forceren naar een ander cluster (of uitsluiten)

CPM kent twee handmatige overrides bovenop de automatische routing:
- **Force / stream-lock** — pin een data stream aan een specifiek cluster.
- **Exclude** — haal een data stream volledig uit CPM-beheer.

Beide worden gelezen door de `cpm-state-manager` watcher; een override werkt pas na
een run van **state-manager → pipeline-manager** (of wacht op de dagelijkse cron 00:15).
Alle voorbeelden zijn Dev Tools / `curl` op de centrale ES.

> **Getest op 2026-07-03**: `logs-system.auth-default` geforceerd van central → remote-a
> (topic verhuisde in de Logstash-pipelines), en `logs-winlog.winlog-default` uitgesloten
> (uit state + pipelines). Beide correct teruggedraaid.

---

## 0. Doelcluster-UUID opzoeken

```
GET cpm-cluster-registry/_search
{ "size": 10, "_source": ["cluster_name", "cluster_id", "active"] }
```
Gebruik de `cluster_id` (UUID) van het doelcluster.

## A. FORCE — stream naar een specifiek cluster

```
PUT cpm-routing-config/_doc/stream_lock:logs-system.auth-default
{
  "config_type": "stream_lock",
  "locked": true,
  "data_stream_type": "logs",
  "dataset": "system.auth",
  "namespace": "default",
  "cluster_id": "<DOEL_CLUSTER_UUID>",
  "pipeline_type": "catchall",
  "reason": "handmatige verplaatsing (ticket X)"
}
```
- Het topic = `<data_stream_type>-<dataset>-<namespace>`. Vul die 3 velden zo dat ze
  samen de data-stream-naam vormen (`logs` / `system.auth` / `default`).
- `pipeline_type`: `catchall` (standaard) of `dedicated`.
- Het doc-`_id` is vrij; `stream_lock:<topic>` is handig om terug te vinden.

**Doorvoeren:**
```
POST _watcher/watch/cpm-state-manager/_execute
{ "record_execution": true, "action_modes": { "_all": "force_execute" } }

POST _watcher/watch/cpm-pipeline-manager/_execute
{ "record_execution": true, "action_modes": { "_all": "force_execute" } }
```
(of `python3 scripts/cpm_run_now.py --only state-manager,pipeline-manager`)

**Controle:** het topic zit nu in pipeline `<dc>_cpm-catchall-<DOEL_UUID>`:
```
GET _logstash/pipeline/<dc>_cpm-catchall-<DOEL_UUID>
GET cpm-pipeline-state/_doc/logs-system.auth-default
```

## B. EXCLUDE — stream uit CPM-beheer halen

Drie manieren, allemaal schrijvend naar dezelfde index `cpm-stream-exclusions`.
`topic_pattern` is de topicnaam == de **data stream-naam**
(`<type>-<dataset>-<namespace>`) en mag één **wildcard** (`*`) bevatten — de
state-manager matcht exact, prefix (`logs-winlog.*`), suffix (`*-tst`) of
prefix+suffix.

### B1. Script — ad hoc, voor één stream (aanbevolen)
```bash
scripts/cpm_exclude_stream.py --insecure add logs-winlog.winlog-default --reason "ticket Y"
scripts/cpm_exclude_stream.py --insecure add '*-tst' --reason "alle test-streams"
scripts/cpm_exclude_stream.py --insecure list
scripts/cpm_exclude_stream.py --insecure check logs-nginx-prod   # welk patroon matcht?
scripts/cpm_exclude_stream.py --insecure remove logs-winlog.winlog-default
```
`check` gebruikt exact dezelfde matchregels als de watcher, dus je kunt vooraf
zien wat een patroon raakt zonder de cyclus te draaien.

### B2. Ansible — declaratief, hoort bij de omgeving
In `inventories/<env>/group_vars/all.yml`:
```yaml
cpm_stream_exclusions:
  - { pattern: "logs-winlog.winlog-default", reason: "ticket Y" }
  - { pattern: "*-tst", reason: "alle test-streams" }

# optioneel: ansible leidend maken en al het overige opruimen
cpm_stream_exclusions_prune: false
```
Uitrollen: `ansible-playbook -i inventories/<env> site.yml --tags exclusions`

Standaard **additief** — exclusions die met B1 of B3 zijn gemaakt blijven staan.
Zet `cpm_stream_exclusions_prune: true` om de inventory leidend te maken; alles
wat niet in de lijst staat wordt dan verwijderd.

### B3. Rauw — Dev Tools
```
PUT cpm-stream-exclusions/_doc/logs-winlog.winlog-default
{
  "topic_pattern": "logs-winlog.winlog-default",
  "reason": "tijdelijk buiten CPM (ticket Y)"
}
```
`updated_at` en `updated_by` hoef je **niet** mee te geven: de index heeft
`default_pipeline: cpm-stream-exclusions-stamp`, die vult `updated_at` met de
huidige tijd (`strict_date_time_no_millis`, UTC) en `updated_by` met `unknown`.
Geef je ze wél mee, dan blijven jouw waarden staan (`override: false`).

Met een wildcard — `*` kan niet in een document-id, dus codeer hem als `_STAR_`:
```
PUT cpm-stream-exclusions/_doc/_STAR_-tst
{
  "topic_pattern": "*-tst",
  "reason": "alle test-streams"
}
```
B1 en B2 gebruiken dezelfde codering, zodat alle drie de routes dezelfde
documenten beheren.

De mapping staat op `dynamic: strict`: alleen `topic_pattern`, `reason`,
`updated_by` en `updated_at` zijn toegestaan. Een extra veld geeft een
`strict_dynamic_mapping_exception`.

Weghalen:
```
DELETE cpm-stream-exclusions/_doc/logs-winlog.winlog-default
DELETE cpm-stream-exclusions/_doc/_STAR_-tst
```

### Doorvoeren en controleren
- Doorvoeren = zelfde run als bij A:
  `scripts/cpm_run_now.py --insecure --only state-manager,pipeline-manager`
- **Controle:** de topic is verdwenen uit `cpm-pipeline-state` én uit de
  `topics => [...]` van alle pipelines. Blijven er voor een cluster géén topics
  over, dan wordt de hele catchall-pipeline verwijderd.

## C. TERUGDRAAIEN — let op: state is *sticky*

Verwijder de override:
```
DELETE cpm-routing-config/_doc/stream_lock:logs-system.auth-default     # force weg
DELETE cpm-stream-exclusions/_doc/logs-winlog.winlog-default            # exclude weg
```
… en draai state-manager + pipeline-manager opnieuw.

**Belangrijk — sticky state:** de bestaande `cpm-pipeline-state` is de bron van
waarheid voor catchall-toewijzingen. Gevolg:
- **Force verwijderen** zet de stream **niet** automatisch terug; hij blijft staan
  waar de lock hem het laatst plaatste. Wil je hem terug: **force expliciet** naar het
  gewenste cluster (en verwijder daarna eventueel de lock — hij blijft dan sticky staan).
- **Exclude verwijderen** brengt de stream alleen terug als hij nog **in de
  stack-monitoring van de laatste 2 uur** zichtbaar is (discovery-venster). Zo niet:
  force hem expliciet naar het gewenste cluster.

## D. Aandachtspunt — verouderde pipelines na een `dc`-wijziging

Pipeline-ids hebben het `dc`-prefix (`<dc>_cpm-catchall-<uuid>`), afgeleid van
`node.attr.dc`. Wijzigt `dc` (bv. van leeg/`default` naar `central`), dan blijven de
**oude-prefix pipelines** (`default_cpm-*`) staan en **consumeren dubbel**. Ruim ze op:
```
GET  _logstash/pipeline            # lijst; zoek oude-prefix ids
DELETE _logstash/pipeline/default_cpm-catchall-<uuid>
```

## Velden-overzicht

| Index | Doel | Kernvelden |
|---|---|---|
| `cpm-routing-config` | force / stream-lock | `config_type:"stream_lock"`, `locked:true`, `data_stream_type`, `dataset`, `namespace`, `cluster_id`, `pipeline_type` |
| `cpm-stream-exclusions` | exclude | `topic_pattern` (mag één `*`), `reason`, `updated_by`, `updated_at` — beheer via `scripts/cpm_exclude_stream.py` of `cpm_stream_exclusions` in de inventory |

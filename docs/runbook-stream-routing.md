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

```
PUT cpm-stream-exclusions/_doc/logs-winlog.winlog-default
{
  "topic_pattern": "logs-winlog.winlog-default",
  "reason": "tijdelijk buiten CPM (ticket Y)",
  "updated_by": "beheerder",
  "updated_at": "2026-07-03T10:00:00Z"
}
```
- `topic_pattern` mag een **wildcard** (`*`) bevatten — de state-manager matcht
  exact, prefix (`logs-winlog.*`), suffix (`*-tst`) of prefix+suffix.
- Doorvoeren = zelfde run als bij A.
- **Controle:** de topic is verdwenen uit `cpm-pipeline-state` én uit alle pipelines.

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
| `cpm-stream-exclusions` | exclude | `topic_pattern` (mag `*`), `reason`, `updated_by`, `updated_at` |

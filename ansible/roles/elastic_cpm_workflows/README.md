# elastic_cpm_workflows

Ansible role that ports the six CPM **Watchers** (role `elastic_cpm`) to
**Elastic 9.4 Workflows**. The workflows reproduce the same functionality but
write to `cpmw-*` indices, so they **run alongside** the existing `cpm-*`
watchers without interfering. The ML anomaly-detector jobs and datafeeds stay
`cpm-*` and are reused (they are not indices).

## What it deploys

- **Indices** (with the original mappings, `cpmw-` prefixed):
  `cpmw-cluster-registry`, `cpmw-scores`, plus 2 seeded docs in
  `cpmw-pipeline-templates`. `cpmw-routing-suggestions` and
  `cpmw-pipeline-state` are created on first write.
- **Six workflows** (pushed via the Kibana Workflows API):

  | Workflow | Trigger | Purpose | Transform |
  |----------|---------|---------|-----------|
  | `cpmw-forecast-trigger` | every 1h | trigger ML `_forecast` on the 4 detectors | none |
  | `cpmw-register-sync` | daily 00:00 | build cluster registry from monitoring | native |
  | `cpmw-scoring` | daily 00:15 | weighted capacity/forecast score per cluster | native |
  | `cpmw-routing-advisor` | daily 00:10 | suggest data-stream moves (two-phase bin-packing) | minimal Painless¹ |
  | `cpmw-state-manager` | daily 00:15 | compute desired pipeline state | native |
  | `cpmw-pipeline-manager` | manual | render + push Logstash pipelines | native |

  ¹ Only `routing-advisor` uses Painless: its assignment algorithm is imperative
  (sorting, mutable state, paired swaps) and runs as a `scripted_metric`
  aggregation. The other five are fully native.

## How the port works

Workflows have no inline-script step and authenticate to Elasticsearch through
the workflow engine itself (no webhook/API-key needed, unlike the watchers).
Each watcher maps to:

- **Inputs** → `elasticsearch.request POST /<index>/_search` steps (the
  `elasticsearch.search` step does not support `aggs`).
- **Transforms** → native ES aggregations (incl. `sum_bucket`, `composite`) plus
  workflow `data.map` / `data.dedupe` / `data.filter` steps and Liquid
  templating. Cross-source joins use `where`/`map`/`first`; set-exclusion uses a
  `keep`-flag computed with `contains` + `data.filter`.
- **Writes** → `foreach` over a structured array + per-doc
  `PUT /<index>/_doc/{id}` (empty-array-safe; no `_bulk` empty-body issues).

Structured data is passed with `${{ steps.x.output }}` (string interpolation uses
`{{ ... }}`).

## Requirements

- A running Elastic 9.4+ stack with Kibana (Workflows is GA in 9.4).
- The `elastic_cpm` role applied first (creates the `cpm-*` ML jobs/datafeeds
  that `forecast-trigger`/`scoring` rely on).
- `.env` in the repo root with `ELASTIC_PASSWORD`, `ES_CENTRAL_PORT`,
  `KIBANA_PORT` (read via `group_vars/all.yml` and this role's defaults).
- Ansible 2.16 (activate the venv first — see repo notes).

## Usage

```bash
source ~/proeftuin/ansible216/ansible216/bin/activate
cd ansible
ansible-playbook workflows.yml
```

The run is **idempotent**: it deletes every existing `cpmw-*` workflow and
recreates them from `files/workflows/*.yml`, so re-running always converges to
exactly six workflows. (Kibana suffixes the derived workflow ids — `-2`, `-3`,
… — after deletes; the names stay clean and unique.)

To run a workflow on demand:

```bash
curl -s -u elastic:$ELASTIC_PASSWORD -H 'kbn-xsrf: true' \
  -X POST "http://localhost:5601/api/workflows/workflow/cpmw-scoring/run" \
  -H 'Content-Type: application/json' -d '{"inputs":{}}'
```

## Verify

```bash
# All six present and valid
curl -s -u elastic:$ELASTIC_PASSWORD -H 'kbn-xsrf: true' \
  "http://localhost:5601/api/workflows?size=100"

# Data produced
curl -sk -u elastic:$ELASTIC_PASSWORD "https://localhost:9200/cpmw-cluster-registry/_search?size=20"
curl -sk -u elastic:$ELASTIC_PASSWORD "https://localhost:9200/cpmw-scores/_search?sort=scored_at:desc&size=1"
curl -sk -u elastic:$ELASTIC_PASSWORD "https://localhost:9200/_logstash/pipeline"
```

## Layout

```
defaults/main.yml                 Kibana connection + index/template lists
tasks/main.yml                    create indices/templates, push workflows (idempotent)
files/json/                       cpmw-* index mappings + pipeline-template docs
files/workflows/*.yml             the six workflow definitions
```

## Notes

- Liquid renders values as strings, so `_source` shows strings; Elasticsearch
  coerces them to the mapped `long`/`float`/`boolean` types on indexing, so
  queries and aggregations behave identically to the watcher output.
- Workflow files must be valid **YAML** (not JSON): Ansible's jinja2-native
  coerces a JSON-string `lookup('file')` into an object and the API rejects it.

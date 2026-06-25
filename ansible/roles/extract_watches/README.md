# extract_watches

Exports CPM watchers from a running Elasticsearch cluster and writes them as
`watcher_*.json.j2` templates under `roles/elastic_cpm/templates/`, ready for
the `elastic_cpm` role to deploy.

## Usage

From `ansible/`:

```bash
# Flex cluster (basic auth from group_vars / .env)
ansible-playbook extract_watches.yml

# Local Docker test cluster (API key)
ansible-playbook -i inventory.extract.docker.ini extract_watches.yml \
  -e extract_watches_api_key=YOUR_BASE64_KEY
```

## Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `extract_watches_elastic_url` | `elastic_base_url` or `https://localhost:9200` | Source cluster URL |
| `extract_watches_auth_method` | `basic` if `elastic_pw` set, else `api_key` | `basic` or `api_key` |
| `extract_watches_api_key` | `""` | Base64 API key when using `api_key` auth |
| `extract_watches_user` / `extract_watches_password` | `elastic_usr` / `elastic_pw` | Basic auth credentials |
| `extract_watches_validate_certs` | `false` | TLS certificate verification |
| `extract_watches_output_dir` | `roles/elastic_cpm/templates` | Destination for `.j2` files |
| `extract_watches_write_json` | `true` | Also write sanitized JSON to `exported_json/` |
| `extract_watches_specs` | see `defaults/main.yml` | Watch ID → template name → Jinja style |

## Template styles

Each watcher uses the same Jinja convention as the existing `elastic_cpm` templates:

- **plain** — `cpm-scoring` (no Jinja variables)
- **simple** — `cpm-forecast-trigger` (direct `{{ webhook_host }}` / API key)
- **inline** — routing-advisor, register-sync (`{% raw %}` with inline breaks)
- **split** — pipeline-manager (host/auth split per webhook)
- **scheme** — state-manager (scheme+host block per webhook)

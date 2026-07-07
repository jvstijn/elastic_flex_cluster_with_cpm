# Elastic Flex CPM

Cluster Pipeline Manager — Ansible deployment, Kibana plugin, and supporting tooling.

| Path | Purpose |
|------|---------|
| [`ansible/`](ansible/README.md) | Deploy CPM to central Elasticsearch (`site.yml`, `bootstrap.yml`) |
| [`kibana_plugin/`](kibana_plugin/README.md) | Stack Management → Cluster Pipeline Manager UI |
| [`scripts/`](scripts/) | Kibana plugin build, reference-stack utilities |
| [`docker-local/`](docker-local/README.md) | Local 3-cluster Docker Compose demo stack |

## Quick links

```bash
# Production / kaposi reference stack
cd ansible && ./scripts/setup_local.sh && ansible-playbook site.yml

# Local Docker demo (3 clusters on localhost)
cd docker-local && cp .env.example .env && docker compose up -d
cd ansible
ansible-playbook -i inventories/local site.yml
```

CPM config source of truth: [`../../cpm/cpm_configs.json`](../../cpm/cpm_configs.json) (repo root).

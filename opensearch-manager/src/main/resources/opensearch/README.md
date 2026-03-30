# OpenSearch ILM and templates (Pipestream)

These JSON snippets are **reference** definitions for operators. Apply them with the OpenSearch REST API or Dashboards Index Management plugin:

- **`ilm-pipestream-repository-events.json`** — ISM policy with **30-day** minimum index age before transition to delete. Adjust `index_patterns` to match your actual index naming.
- **`component-template-pipestream-repository-fields.json`** — Common fields for **`datasource_id`**, **`retention_intent_days`**, and tenant keys for dashboards and per-datasource retention jobs.

`opensearch-manager` indexes documents at runtime; ensure index templates in the cluster include these fields (merge this component template into your composable index template).

Example (cluster-specific):

```bash
curl -X PUT "https://opensearch:9200/_plugins/_ism/policies/pipestream-repo-30d" \
  -H 'Content-Type: application/json' \
  -d @ilm-pipestream-repository-events.json
```

Note: OpenSearch 2.x uses **ISM** (`_plugins/_ism/`); some distributions use `_opendistro/_ism/`. Adjust paths for your version.

# Pipestream pipeline-events observability

Version-controlled OpenSearch + OpenSearch Dashboards setup for the
`pipeline-events-*` indices written by `PipelineEventConsumer`.

Re-applying after switching to a new cluster is two commands.

## What's in here

```
observability/
├── templates/
│   └── pipeline-events.json              # OS index template (mappings + alias)
├── queries/
│   ├── 01-throughput-per-instance.json   # raw query DSL, paste into Dev Tools
│   ├── 02-step-latency-percentiles.json
│   ├── 03-partial-rate-per-module.json
│   └── 04-doc-end-to-end-latency.json
├── dashboards/
│   └── pipestream-pipeline-events.ndjson # OSD saved-objects export
└── scripts/
    ├── apply-template.sh                  # PUT _index_template
    └── import-dashboards.sh               # POST _dashboards/api/saved_objects/_import
```

## Switching clusters / first-time setup

```bash
cd observability/scripts

# 1. Apply the index template (fixes date types + keyword fields).
#    Re-runnable; idempotent.
OS_URL=http://localhost:9200 ./apply-template.sh

# 2. Import the dashboard into OpenSearch Dashboards.
OSD_URL=http://localhost:5601 ./import-dashboards.sh
```

For secured clusters:

```bash
OS_URL=https://os.prod:9200  OS_USER=admin  OS_PASS='...'  OS_INSECURE=1  ./apply-template.sh
OSD_URL=https://dashboards.prod  OSD_USER=admin  OSD_PASS='...'  OSD_INSECURE=1  ./import-dashboards.sh
```

After import: open the dashboard at `${OSD_URL}/app/dashboards` →
**Pipestream / Pipeline Events**.

## Why an index template is required

`PipelineEventConsumer` writes `event_timestamp` and `indexed_at` as raw
`long` epoch-ms values, and string fields without explicit mappings.
With dynamic mapping that gives you `long` (not `date`) for timestamps
and `text+keyword` multi-fields everywhere else.

The template fixes both:

- `event_timestamp` and `indexed_at` map as `date` (`format: epoch_millis`)
  so the OSD time picker works without the user having to flip
  "epoch_millis" toggles in the index pattern.
- `node_id`, `module_id`, `service_instance_id`, `status`, `error_code`,
  `document_id`, `account_id`, `stream_id`, `graph_id` all map as plain
  `keyword` so terms-aggs in dashboards don't need a `.keyword` suffix.
- `error_message` and log `message` stay as `text` for full-text search.

> Existing monthly indices keep their dynamic mappings. The template
> only affects new monthly indices going forward. To retroactively fix
> a single month, reindex it into a fresh index that picks up the
> template (Q4 example below).

## What's on the dashboard

| Panel | Question it answers | Source query |
|---|---|---|
| Throughput per instance | "Is one engine instance starved or hot?" | `queries/01-throughput-per-instance.json` |
| Step latency p50/p95/p99 | "Which step is slow?" | `queries/02-step-latency-percentiles.json` |
| PARTIAL rate per module | "Are modules silently degrading throughput?" | `queries/03-partial-rate-per-module.json` |
| Slowest docs end-to-end | "Which docs are outliers, and where do they spend their wall-clock?" | `queries/04-doc-end-to-end-latency.json` |

All four panels honor the dashboard time picker (`event_timestamp`)
and respect any KQL filter you set on the dashboard (e.g. filter by
`account_id` to scope to one tenant).

## Visualization implementation note

The four visualizations are **Vega-Lite specs** (`type: vega`), not Lens
or Visualize Builder. This is a deliberate portability choice:

- Vega specs are self-contained JSON. They survive OSD major-version
  upgrades and round-trip through saved-objects export/import without
  schema migrations breaking the spec.
- Lens visualizations carry version-specific `state` blobs that
  frequently break across OSD upgrades and across cluster moves.

If you want to edit a panel: open it in Dashboards, click the gear → Edit
visualization, change the spec, save. Then re-export with:

```bash
OSD_URL=http://localhost:5601 \
  curl -s -X POST "$OSD_URL/api/saved_objects/_export" \
       -H 'osd-xsrf: true' -H 'Content-Type: application/json' \
       -d '{"objects":[{"type":"dashboard","id":"pipestream-pipeline-events"}],"includeReferencesDeep":true}' \
  > dashboards/pipestream-pipeline-events.ndjson
```

…and commit the result.

## Running the raw queries directly

The four files in `queries/` are runnable as-is from OSD Dev Tools:

```
GET pipeline-events-*/_search
{ ...paste the body of one of the files (drop the leading "_doc" line)... }
```

They are intentionally portable across `curl`, Dev Tools, and the
OpenSearch SDKs — no Dashboards-specific syntax.

# Pipeline Events — Analysis Queries

**Index:** `pipeline-events-{YYYY.MM}` (monthly rollover, see `PipelineEventConsumer.java:110-112`)
**Producer:** `PipelineEventConsumer` consuming Kafka topic `pipeline-events`, payload `StepExecutionRecord` (proto), one record per node × document execution.
**Cluster:** wherever `opensearch.hosts` resolves (default in `%dev`: `opensearch.rokkon.com:443`).

This doc captures the queries we use to investigate per-step latency tails ("kinks") in pipeline crawls. Intended as the source of truth for a future dashboard.

---

## 1. Schema reference

Field shapes inferred from the live mapping (May 2026 index). Most string fields are `text` with a `.keyword` sub-field — **always use `.keyword` for term/exact filters and aggregations**.

| Field | Type | Notes |
|---|---|---|
| `document_id` | text + `.keyword` | Pipeline document id (typically `{datasourceId}:{rowPk}`) |
| `account_id` | text + `.keyword` | The graph-crawl account; one crawl ⇒ one account_id |
| `stream_id` | text + `.keyword` | Per-stream id (one per intake handoff, persists across hops) |
| `graph_id` | text + `.keyword` | Pipeline graph id |
| `node_id` | text + `.keyword` | **Step name** — `chunker`, `embedder`, `opensearch-sink`, `tap-*`, etc. |
| `module_id` | text + `.keyword` | Module that ran (often equals `node_id` but can differ) |
| `hop_number` | long | Position in the per-stream execution path (1 = entry node) |
| `status` | text + `.keyword` | `SUCCESS` / `PARTIAL` / `FAILURE` |
| `duration_ms` | long | `endTime - startTime` for this step |
| `service_instance_id` | text + `.keyword` | Engine instance that ran the step (HOSTNAME-derived) |
| `event_timestamp` | long | **epoch milliseconds** — NOT a `date` mapping; date math (`now-1h`) does NOT work here |
| `indexed_at` | long | epoch ms when consumer queued for indexing |
| `error_code`, `error_message` | text + `.keyword` | Present only on failure (`step.hasErrorInfo()`) |
| `log_entries[]` | nested | Up to 40 entries, each with `level`, `source`, `message`, `timestamp_epoch_ms` |
| `log_entry_count`, `log_entries_stored_count`, `log_entries_dropped_count`, `log_entries_total_chars`, `log_entries_truncated` | long / boolean | Tracking that captures truncation if the full payload exceeded `MAX_TOTAL_LOG_CHARS=12000` |
| `semantic_manager_telemetry.*` | dynamic | Parsed `key=value` tokens from any log entry prefixed `telemetry semantic_manager` |

### Important gotchas

- **`event_timestamp` is `long`, not `date`.** OpenSearch dynamic mapping picked numeric. Range queries must use raw epoch ms (`{ "gte": 1778336400000 }`), not `now-1h`. If we want date math we need a date alias field added in `PipelineEventConsumer` (separate change).
- **`status` is `text` with `.keyword`.** `term: { status: "SUCCESS" }` will not match because the analyzer lowercases. Use `status.keyword`.
- **One row per (node × document) execution.** A retried call produces a *new* row with the same `document_id`/`stream_id`. Counting `document_id` cardinality at a given node tells you docs-attempted, not docs-succeeded. Use `status.keyword:SUCCESS` filter for success counts.
- **Index `pipeline-events-2026.05` has 6 primary shards.** Aggregations on high-cardinality fields (e.g. `document_id.keyword`) need `size` tuned and may need `composite` for full enumeration.

---

## 2. Reading the patterns

The hot-path symptom we hunt is a **bimodal duration histogram**: a tight bulk cluster + an empty middle band + a sharp outlier spike. That shape says "most docs are processed at a healthy rate, but a small set hit a contention/queue/deadline wall." Examples seen in production:

- **Chunker pre-fix (2026-05-09):** bulk 10-30s, 0 in 30-60s, 2 docs at ~90s.
  Cause: process-wide shared `parallelSentenceExecutor` merged sentence-tag tasks across 1000 concurrent requests; the last tasks of the unlucky two docs sat at the back of a 200K-task queue. Fixed by removing the fan-out; sentences now run sequentially within the caller VT.
- **Embedder (2026-05-09):** ~10% duplicate rate at sidecar, throughput 9.6/s overall.
  Cause: `EmbedderGrpcImpl.processData` blocks on `await().indefinitely()` which doesn't propagate gRPC cancellation; engine 90s deadline trips → engine retries → original Mutiny chain keeps doing work → up to 3× zombie work for the same doc per call. See `cross-request-collapser-design.md`.
- **opensearch-sink (2026-05-09):** bulk 3-7s (926 docs), 0 in 15-60s, 9 docs at ~91s.
  Same architectural shape. Likely cause: shared connection/queue saturation against the OpenSearch bulk endpoint; investigation pending.

The 90s ceiling on the outliers in all three is suspicious — that's `pipestream.module.retry.call-deadline-ms = 90000` in `engine/src/main/resources/application.properties`. Calls that brush against it are about to be retried; calls that sit beyond it are the engine giving up. **Any time the outlier spike sits at ~90s, the deadline is doing the work, not the module.**

---

## 3. Core queries

Each query block below is named so dashboard widgets can reference them. All use the `pipeline-events-{YYYY.MM}` index for the relevant month — for cross-month queries use `pipeline-events-*`.

### 3.1 `per-step-status-and-latency` — one crawl, all steps

Use as the headline panel for a crawl. Shows row count per step, status breakdown, and latency percentiles. Reveals which step (if any) has a tail.

```json
GET pipeline-events-2026.05/_search?size=0
{
  "query": { "term": { "account_id.keyword": "{{account_id}}" } },
  "aggs": {
    "by_node": {
      "terms":  { "field": "node_id.keyword", "size": 50 },
      "aggs": {
        "by_status": { "terms": { "field": "status.keyword", "size": 5 } },
        "lat":       { "percentiles": { "field": "duration_ms",
                                        "percents": [50, 90, 95, 99, 100] } }
      }
    }
  }
}
```

**Dashboard widget:** stacked-bar (status counts) + sparkline of percentiles per step.

**Read it:** anywhere `p99 / p95 > 3` is a tail to investigate. Anywhere `max == p100 ≈ 90000` is hitting the engine deadline.

### 3.2 `slowest-docs-at-step` — top-N latency offenders for one (account, step)

Drill-in for a specific step that 3.1 flagged.

```json
GET pipeline-events-2026.05/_search
{
  "size": 50,
  "_source": ["document_id", "stream_id", "duration_ms", "event_timestamp",
              "service_instance_id", "status", "error_code", "error_message"],
  "query": {
    "bool": {
      "filter": [
        { "term":  { "account_id.keyword": "{{account_id}}" } },
        { "term":  { "node_id.keyword":    "{{node_id}}"    } },
        { "range": { "duration_ms":        { "gte": {{slow_ms}} } } }
      ]
    }
  },
  "sort": [{ "duration_ms": "desc" }]
}
```

`{{slow_ms}}` should be ~`p95 × 2` of the step's healthy range. Sensible defaults today: chunker 5000, embedder 5000, opensearch-sink 10000.

**Dashboard widget:** sortable table with per-row drill into 3.4.

### 3.3 `latency-histogram` — bimodal-shape detector

The single most useful diagnostic — confirms whether a tail is "long-tail of slow docs" (smooth) or "queue-merging contention" (bimodal).

```json
GET pipeline-events-2026.05/_search?size=0
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "account_id.keyword": "{{account_id}}" } },
        { "term": { "node_id.keyword":    "{{node_id}}"    } }
      ]
    }
  },
  "aggs": {
    "buckets": {
      "range": {
        "field": "duration_ms",
        "ranges": [
          { "to":   1000  },
          { "from": 1000,  "to": 3000  },
          { "from": 3000,  "to": 7000  },
          { "from": 7000,  "to": 15000 },
          { "from": 15000, "to": 30000 },
          { "from": 30000, "to": 60000 },
          { "from": 60000, "to": 90000 },
          { "from": 90000               }
        ]
      }
    }
  }
}
```

**Dashboard widget:** horizontal bar chart, log scale.

**Read it:** an empty middle bucket flanked by two populated buckets is the contention signature. A single mode tapering to the right is healthy long-tail behavior — different problem class.

### 3.4 `doc-trace` — full per-step path for one document

Shows every node a single doc visited with duration. Distinguishes "doc was slow at multiple steps" (likely big doc) from "doc was only slow at one step" (likely backend-side contention at that step).

```json
GET pipeline-events-2026.05/_search
{
  "size": 200,
  "_source": ["node_id", "module_id", "hop_number", "status", "duration_ms",
              "event_timestamp", "service_instance_id", "stream_id"],
  "query": { "term": { "document_id.keyword": "{{document_id}}" } },
  "sort":  [{ "event_timestamp": "asc" }]
}
```

Sort by `event_timestamp` (not `hop_number`) so retries/duplicates show in actual chronological order.

**Dashboard widget:** waterfall / timeline view.

### 3.5 `outliers-by-instance` — same-instance vs cross-instance contention

If all the slow docs ran on one engine instance → per-instance resource issue (carrier pool, channel pool, GC).
If scattered across instances → shared backend (downstream module, OpenSearch, Kafka).

```json
GET pipeline-events-2026.05/_search?size=0
{
  "query": {
    "bool": {
      "filter": [
        { "term":  { "account_id.keyword": "{{account_id}}" } },
        { "term":  { "node_id.keyword":    "{{node_id}}"    } },
        { "range": { "duration_ms":        { "gte": {{slow_ms}} } } }
      ]
    }
  },
  "aggs": {
    "by_instance": { "terms": { "field": "service_instance_id.keyword", "size": 20 } }
  }
}
```

**Dashboard widget:** small horizontal-bar.

### 3.6 `cross-account-step-survey` — find which crawls had a kink at this step

Catches regressions across multiple recent crawls without naming an account.

```json
GET pipeline-events-2026.05/_search?size=0
{
  "query": {
    "bool": {
      "filter": [
        { "term":  { "node_id.keyword":   "{{node_id}}" } },
        { "range": { "event_timestamp":   { "gte": {{since_epoch_ms}} } } }
      ]
    }
  },
  "aggs": {
    "by_account": {
      "terms": { "field": "account_id.keyword", "size": 30 },
      "aggs": {
        "lat": { "percentiles": { "field": "duration_ms", "percents": [50, 95, 99, 100] } },
        "outliers": {
          "filter": { "range": { "duration_ms": { "gte": {{slow_ms}} } } },
          "aggs": {
            "docs": {
              "top_hits": {
                "size": 10,
                "_source": ["document_id", "duration_ms"],
                "sort": [{ "duration_ms": "desc" }]
              }
            }
          }
        }
      }
    }
  }
}
```

**Dashboard widget:** matrix (account × percentile) with outlier-count column and expander for top-doc rows.

### 3.7 `failures-with-error` — every non-SUCCESS row in time window

For triage / on-call.

```json
GET pipeline-events-2026.05/_search
{
  "size": 100,
  "_source": ["document_id", "node_id", "module_id", "status", "error_code",
              "error_message", "duration_ms", "event_timestamp",
              "service_instance_id", "log_entries"],
  "query": {
    "bool": {
      "must_not": [
        { "term": { "status.keyword": "SUCCESS" } }
      ],
      "filter": [
        { "range": { "event_timestamp": { "gte": {{since_epoch_ms}} } } }
      ]
    }
  },
  "sort": [{ "event_timestamp": "desc" }]
}
```

### 3.8 `retry-detector` — same (stream, node) appearing more than once

Engine retries (or sidecar redeliveries) produce duplicate rows for the same `(stream_id, node_id)`. A high count here is a strong signal of zombie-work amplification (e.g., the chunker / embedder cancellation issues we identified).

```json
GET pipeline-events-2026.05/_search?size=0
{
  "query": {
    "bool": {
      "filter": [
        { "term":  { "account_id.keyword": "{{account_id}}" } },
        { "term":  { "node_id.keyword":    "{{node_id}}"    } }
      ]
    }
  },
  "aggs": {
    "by_stream": {
      "terms": { "field": "stream_id.keyword", "size": 10000, "min_doc_count": 2 },
      "aggs": {
        "attempts": { "value_count": { "field": "stream_id.keyword" } }
      }
    }
  }
}
```

`min_doc_count: 2` means only streams with ≥2 attempts at this node show up. Any non-empty result here is worth investigation.

---

## 4. Suggested dashboard layout (future)

Single dashboard, parameterised by `account_id` (defaults to "most recent") and `time window`.

**Top row — single-crawl health**

| Widget | Source query |
|---|---|
| Per-step status counts (stacked bar) | `per-step-status-and-latency` |
| Per-step p50/p95/p99/max (line) | `per-step-status-and-latency` |
| Total docs in crawl (counter) | `per-step-status-and-latency`, sum of `tap-*` SUCCESS |

**Middle row — kink detector (one panel per step)**

| Widget | Source query |
|---|---|
| Bimodal histogram (bar) | `latency-histogram` |
| Top-20 slowest docs (table, drill → trace) | `slowest-docs-at-step` |
| Outliers by engine instance (bar) | `outliers-by-instance` |

**Bottom row — fleet-level**

| Widget | Source query |
|---|---|
| Cross-account step health (matrix) | `cross-account-step-survey` |
| Recent failures (table) | `failures-with-error` |
| Retry hotspots (table, per node) | `retry-detector` |

**Drill-in panel (modal/route):** `doc-trace` keyed on a clicked `document_id`.

---

## 5. Open questions

1. **Add a `date`-mapped alias for `event_timestamp`?** Would unblock OpenSearch Dashboards' built-in time picker and `now-1h` filters. Cost: explicit index template (current index uses dynamic mapping). Recommend doing this for the next monthly index.
2. **Aggregating across months.** `pipeline-events-*` works today but performance on multi-month queries hasn't been validated. May need a write-alias + ILM rollover.
3. **Capture the engine's `dispatched_at_epoch_ms` as a separate field?** Would let us compute *queue time* (dispatched → step-start) vs *service time* (step-start → step-end) per event. Currently we only see service time via `duration_ms`. Useful for distinguishing "module slow" from "module backed up before I even started".
4. **Crawl-completion event.** No dedicated row marks "the crawl finished." Today we infer it from the latest `tap-*` event for an account. A first-class crawl-finished row would simplify dashboard queries materially.
5. **Index lifecycle.** Monthly index keeps growing; no rollover or retention policy currently configured. May want size-based ISM policy before the May index gets very large (currently 104MB / 177k docs).

---

## 6. Reference links

- Producer: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/kafka/PipelineEventConsumer.java`
- Bulk indexer: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/bulk/BulkQueueSetBean.java`
- Indexing service: `opensearch-manager/src/main/java/ai/pipestream/schemamanager/OpenSearchIndexingService.java`
- Schema source: `StepExecutionRecord` proto (in `pipestream-protos`)
- Engine emitter: `pipestream-engine/.../telemetry/TelemetryEmitter.java` (fires `pipeline-events` Kafka publish per `processNodeLogic` completion)
- Engine deadline that explains 90s outlier spikes: `pipestream.module.retry.call-deadline-ms` in `pipestream-engine/src/main/resources/application.properties`

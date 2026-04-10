# Crawl Event Observability Guide

This guide shows how to inspect crawl progress and timing directly from `pipeline-events-*`.

## Which URL to query

OpenSearch **HTTP REST** is almost always on **port 9200**, e.g. `https://<host>:9200/...`, matching `opensearch.hosts` (`OPENSEARCH_HOST` + `OPENSEARCH_PORT`, default port **9200**) and `opensearch.protocol`. The site root on **port 443** is often **OpenSearch Dashboards** or another UI; `curl https://<host>/` is **not** the same as the indexing API. Use the same base URL your `opensearch-manager` uses.

## What is stored per event

Each event document includes:

- Core routing fields: `document_id`, `stream_id`, `graph_id`, `node_id`, `module_id`, `status`, `duration_ms`
- Error fields when present: `error_code`, `error_message`
- Log metadata: `log_entry_count`, `log_entries_stored_count`, `log_entries_dropped_count`, `log_entries_total_chars`, `log_entries_truncated`
- Capped log payload: `log_entries[]` (timestamp, level, source, message, optional module/engine origin)
- Semantic telemetry when emitted: `semantic_manager_telemetry.*`

## Log entry caps

To keep indexing safe and predictable:

- Max stored log entries per event: `40`
- Max total stored log message chars per event: `12000`
- Max chars per individual log message: `1000`

When truncation happens:

- `log_entries_truncated: true`
- `log_entries_dropped_count > 0` when entries were omitted
- Individual entries may include `message_truncated: true`

## 1) Confirm events are arriving for a crawl

Use either `stream_id` or `graph_id` as your primary filter.

```json
GET /pipeline-events-*/_search
{
  "size": 20,
  "sort": [{"event_timestamp": "desc"}],
  "_source": [
    "event_timestamp",
    "document_id",
    "stream_id",
    "graph_id",
    "node_id",
    "module_id",
    "status",
    "duration_ms",
    "log_entry_count"
  ],
  "query": {
    "bool": {
      "filter": [
        {"term": {"stream_id.keyword": "<stream-id>"}}
      ]
    }
  }
}
```

## 2) Measure bottlenecks by node

```json
GET /pipeline-events-*/_search
{
  "size": 0,
  "query": {
    "term": {"stream_id.keyword": "<stream-id>"}
  },
  "aggs": {
    "by_node": {
      "terms": {"field": "node_id.keyword", "size": 50},
      "aggs": {
        "count": {"value_count": {"field": "document_id.keyword"}},
        "avg_ms": {"avg": {"field": "duration_ms"}},
        "p95_ms": {"percentiles": {"field": "duration_ms", "percents": [50, 95, 99]}},
        "max_ms": {"max": {"field": "duration_ms"}}
      }
    }
  }
}
```

## 3) Track semantic-manager telemetry

```json
GET /pipeline-events-*/_search
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        {"term": {"stream_id.keyword": "<stream-id>"}},
        {"term": {"module_id.keyword": "semantic-manager"}}
      ]
    }
  },
  "aggs": {
    "outcomes": {
      "terms": {"field": "semantic_manager_telemetry.outcome.keyword"},
      "aggs": {
        "avg_duration_ms": {"avg": {"field": "semantic_manager_telemetry.duration_ms"}},
        "p95_duration_ms": {
          "percentiles": {
            "field": "semantic_manager_telemetry.duration_ms",
            "percents": [50, 95, 99]
          }
        },
        "avg_chunk_count": {"avg": {"field": "semantic_manager_telemetry.chunk_count"}},
        "avg_failed_embeddings": {"avg": {"field": "semantic_manager_telemetry.failed_embeddings"}}
      }
    }
  }
}
```

## 4) Inspect slow documents with full context

```json
GET /pipeline-events-*/_search
{
  "size": 25,
  "sort": [{"duration_ms": "desc"}],
  "_source": [
    "document_id",
    "stream_id",
    "node_id",
    "module_id",
    "status",
    "duration_ms",
    "error_message",
    "semantic_manager_telemetry",
    "log_entries"
  ],
  "query": {
    "bool": {
      "filter": [
        {"term": {"stream_id.keyword": "<stream-id>"}}
      ]
    }
  }
}
```

## 5) Check whether logs are being capped

```json
GET /pipeline-events-*/_search
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        {"term": {"stream_id.keyword": "<stream-id>"}},
        {"term": {"log_entries_truncated": true}}
      ]
    }
  },
  "aggs": {
    "avg_dropped": {"avg": {"field": "log_entries_dropped_count"}},
    "max_dropped": {"max": {"field": "log_entries_dropped_count"}}
  }
}
```

## 6) Pull phase timing lines from stored logs

Semantic-manager phase timing is emitted as log lines (`Phase 0 timing`, `Phase 1 timing`, `Phase 2 timing`, `Semantic orchestration complete`).

```json
GET /pipeline-events-*/_search
{
  "size": 50,
  "sort": [{"event_timestamp": "desc"}],
  "_source": [
    "document_id",
    "node_id",
    "module_id",
    "log_entries.message"
  ],
  "query": {
    "bool": {
      "filter": [
        {"term": {"stream_id.keyword": "<stream-id>"}}
      ],
      "must": [
        {
          "bool": {
            "should": [
              {"wildcard": {"log_entries.message.keyword": "*Phase 0 timing*"}},
              {"wildcard": {"log_entries.message.keyword": "*Phase 1 timing*"}},
              {"wildcard": {"log_entries.message.keyword": "*Phase 2 timing*"}},
              {"wildcard": {"log_entries.message.keyword": "*Semantic orchestration complete*"}}
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  }
}
```

## Practical workflow

1. Filter by `stream_id` for one crawl run.
2. Run node-level aggregation to find highest `p95`/`max` duration.
3. Inspect top slow docs for those nodes.
4. Check `semantic_manager_telemetry` and `log_entries` on slow docs.
5. Confirm whether log truncation is hiding detail (`log_entries_truncated`).

# OpenSearch Vector Indexing Performance Tuning

Reference: https://docs.opensearch.org/latest/vector-search/performance-tuning-indexing/

## Current Architecture

- Engine: **Lucene** (required for hybrid search — keyword + vector in one query)
- Algorithm: HNSW
- Space type: cosinesimil
- Vector dimensions: 384 (MiniLM-L6-v2)
- Chunk indices created dynamically by `ChunkCombinedIndexingStrategy`

## Problem Statement

100 JDBC docs produce ~38,000 indexed items across 7 indices (base doc + 6 chunk strategies).
On a 1GB-heap single-node Docker OpenSearch, this took 438s. The opensearch-sink pipeline step
averages 29.3s per document (P95: 86.5s). This is the primary pipeline bottleneck — it applies
backpressure to semantic-manager upstream, making embedding appear slow when it's actually
waiting on indexing.

## Tuning Knobs (Lucene Engine, HNSW)

### 1. Disable refresh_interval During Bulk Indexing

Default is 1 second. Every refresh creates a new segment. During bulk ingestion, disable it
to avoid creating hundreds of tiny segments that trigger expensive merges.

```
PUT /<index_name>/_settings
{ "index": { "refresh_interval": "-1" } }
```

**Re-enable after indexing completes.** Our implementation should set `-1` at index creation
and re-enable (e.g., `30s`) after the crawl finishes or via a gRPC endpoint.

### 2. Disable Replicas During Indexing

Each replica shard builds its own HNSW graph independently — doubling construction cost.
Set `number_of_replicas: 0` during bulk indexing. After indexing, set replicas back to 1+
and OpenSearch copies the serialized graph directly (no reconstruction).

```
PUT /<index_name>/_settings
{ "index": { "number_of_replicas": 0 } }
```

### 3. Increase KNN Indexing Thread Count

`knn.algo_param.index_thread_qty` controls threads for native library index construction.
Default appears to be very low. On a multi-core machine, increase to utilize available cores.

```
PUT /_cluster/settings
{ "persistent": { "knn.algo_param.index_thread_qty": 8 } }
```

This is a **cluster-level dynamic setting** — no restart needed. Monitor CPU and adjust.

### 4. Increase Number of Shards for Vector Indices

Each shard maintains its own independent HNSW graph. Multiple shards = parallel graph
construction across cores. For large vector indices (10K+ vectors), 4 shards on a
multi-core machine is a reasonable starting point.

Set at index creation time:
```json
{
  "settings": {
    "number_of_shards": 4,
    "number_of_replicas": 0,
    "index.knn": true
  }
}
```

### 5. HNSW Construction Parameters

Set explicitly at field mapping time. Lucene defaults may be higher than needed.

| Parameter | Default | Recommended (indexing speed) | Effect |
|-----------|---------|----------------------------|--------|
| `ef_construction` | 100 (Lucene) | 100 | Size of dynamic candidate list during graph build. Lower = faster indexing, slightly lower recall. |
| `m` | 16 | 16 | Bidirectional links per node. Lower = faster builds + less memory, lower recall. Range 2-100. |

```json
{
  "type": "knn_vector",
  "dimension": 384,
  "space_type": "cosinesimil",
  "method": {
    "name": "hnsw",
    "engine": "lucene",
    "parameters": {
      "ef_construction": 100,
      "m": 16
    }
  }
}
```

### 6. Derived Vector Source (OpenSearch 3.0+)

Enabled by default in OpenSearch 3.0+. Prevents vectors from being stored in `_source`
field while maintaining full functionality (update, reindex, etc.). Significantly reduces
storage for vector-heavy indices.

### 7. (Expert) Defer HNSW Graph Construction

For bulk-upload-then-search workloads, skip HNSW graph construction during indexing entirely
and build it once via force merge afterward.

**Step 1:** Disable graph construction:
```
PUT /test-index/_settings
{ "index.knn.advanced.approximate_threshold": "-1" }
```

**Step 2:** Bulk index all data (searches during this phase use exact kNN — slow but works).

**Step 3:** Re-enable graph construction:
```
PUT /test-index/_settings
{ "index.knn.advanced.approximate_threshold": "0" }
```

**Step 4:** Force merge to single segment (builds HNSW graph once):
```
POST /test-index/_forcemerge?max_num_segments=1
```

This is the fastest possible bulk indexing path but requires that no approximate searches
happen during ingestion. Suitable for initial data loads and re-crawls.

## Implementation Plan

### Phase 1: Index Creation Settings (in ChunkCombinedIndexingStrategy)

Add configurable settings to chunk index creation:

```java
// At index creation time in ensureFlatKnnField():
openSearchAsyncClient.indices().create(c -> c
    .index(chunkIndexName)
    .settings(s -> s
        .knn(true)
        .numberOfShards(String.valueOf(config.numberOfShards()))  // default: 4
        .numberOfReplicas("0")                                     // 0 during indexing
        .refreshInterval(ri -> ri.time("-1"))                      // disabled during indexing
    )
    .mappings(m -> buildNlpAnalysisMappings(m))
)
```

HNSW parameters explicitly set in putMapping:
```java
.knnVector(knn -> knn
    .dimension(dimensions)
    .method(method -> method
        .name("hnsw")
        .engine("lucene")
        .spaceType("cosinesimil")
        .parameters(Map.of(
            "ef_construction", config.efConstruction(),  // default: 100
            "m", config.hnswM()                          // default: 16
        ))
    )
)
```

### Phase 2: Post-Indexing Optimization Endpoint

Add a gRPC RPC `OptimizeIndicesAfterBulk` that:
1. Sets `refresh_interval` back to `30s`
2. Sets `number_of_replicas` back to 1
3. Optionally calls `_forcemerge?max_num_segments=1`

This can be triggered by the E2E runner after crawl completes, or by the engine
when a graph execution finishes.

### Phase 3: Cluster-Level Tuning

Apply once to the cluster (e.g., in Docker compose or init script):
```
PUT /_cluster/settings
{
  "persistent": {
    "knn.algo_param.index_thread_qty": 8
  }
}
```

### Phase 4: Expert Mode (Deferred Graph Construction)

For re-crawl scenarios, add an option to set `approximate_threshold: -1` during
bulk indexing, then force merge after. This would be a flag on the crawl request
or graph execution config.

## Configuration (application.properties)

```properties
# KNN index creation defaults
knn-index.number-of-shards=4
knn-index.ef-construction=100
knn-index.hnsw-m=16
# Post-indexing
knn-index.post-bulk-refresh-interval=30s
knn-index.post-bulk-replicas=1
```

## Monitoring

Use pipeline-events index to measure per-node duration:
```json
GET /pipeline-events-*/_search
{
  "size": 0,
  "query": {"term": {"graph_id.keyword": "<graph-id>"}},
  "aggs": {
    "by_node": {
      "terms": {"field": "node_id.keyword"},
      "aggs": {
        "avg_ms": {"avg": {"field": "duration_ms"}},
        "p95_ms": {"percentiles": {"field": "duration_ms", "percents": [50, 95, 99]}},
        "total_ms": {"sum": {"field": "duration_ms"}}
      }
    }
  }
}
```

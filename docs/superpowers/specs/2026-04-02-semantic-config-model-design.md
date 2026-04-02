# Semantic Config Model — Separate VectorSet Type for Semantic Chunking

## Goal

Add a `semantic_config` table to opensearch-manager that models semantic chunking as a first-class entity, separate from the standard `chunker_config`. Fix the current bug where semantic processing results (sentence, paragraph, document granularities) fail at indexing time because opensearch-manager tries to resolve them as registered chunker configs.

## Problem

The semantic manager produces 5 result types from a single semantic chunking run:

| Result | chunk_config_id today | Is it a ChunkerConfig? |
|---|---|---|
| Standard token chunks | `"default"` | Yes — registered |
| Semantic topic chunks | `"semantic"` | No — produced by boundary detection |
| Sentence vectors | `"sentence"` | No — NLP sentence split, already embedded |
| Paragraph centroids | `"paragraph"` | No — averaged vectors, not chunked |
| Document centroid | `"document"` | No — single averaged vector |

The opensearch-manager's `resolveOrCreateVectorSet` tries to look up every `chunk_config_id` as a `ChunkerConfigEntity`. The last 4 aren't chunker configs — they're granularity labels for different vector types produced by semantic analysis. The lookup fails with "Chunker config not found: sentence".

## Design Principles

1. **Semantic is additive, not alternative.** Standard M x N chunking (M chunker configs x N embedders) continues unchanged. Semantic adds a family of vector sets on top.
2. **One semantic config = one embedding model.** Sentence vectors, centroids, and semantic chunks all live in the same vector space. Different model = different semantic config.
3. **Pre-registration, not find-or-create.** All VectorSets for a semantic config are created at config registration time. No creation during document flow. No race conditions.
4. **Centroids are free.** Sentence embedding happens for boundary detection anyway. Storing sentence vectors = free. Centroids = averaging existing vectors, zero embedding calls.

## Architecture

### Formula

Total VectorSets per index = **(M x N)** standard + **up to 5** per semantic config

| User intent | Config | VectorSets | Embedding calls |
|---|---|---|---|
| Just chunk it | 1 chunker x 1 embedder | 1 | 1 batch |
| Give me options | 2 chunkers x 2 embedders | 4 | 4 batches |
| Semantic only | 1 semantic config | up to 5 | 2 batches (sentences + semantic chunks) |
| Full analysis | 2x2 standard + 1 semantic | 9 | 6 batches |

### New Table: `semantic_config`

```sql
CREATE TABLE semantic_config (
    id                          VARCHAR(255) PRIMARY KEY,
    name                        VARCHAR(255) NOT NULL UNIQUE,
    config_id                   VARCHAR(255) NOT NULL UNIQUE,

    -- FK to the one embedding model used for all vector types
    embedding_model_id          VARCHAR(255) NOT NULL
        REFERENCES embedding_model_config(id),

    -- Semantic boundary detection parameters
    similarity_threshold        REAL NOT NULL DEFAULT 0.5,
    percentile_threshold        INTEGER NOT NULL DEFAULT 20,
    min_chunk_sentences         INTEGER NOT NULL DEFAULT 2,
    max_chunk_sentences         INTEGER NOT NULL DEFAULT 30,

    -- Which output granularities to produce
    store_sentence_vectors      BOOLEAN NOT NULL DEFAULT TRUE,
    compute_centroids           BOOLEAN NOT NULL DEFAULT TRUE,

    -- Extensibility (section_source_priority, future params)
    config_json                 JSONB,

    created_at                  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMP NOT NULL DEFAULT now()
);
```

### Modified Table: `vector_set`

Add two nullable columns:

```sql
ALTER TABLE vector_set
    ADD COLUMN semantic_config_id VARCHAR(255)
        REFERENCES semantic_config(id),
    ADD COLUMN granularity VARCHAR(50);

-- Make chunker_config_id nullable (currently NOT NULL)
ALTER TABLE vector_set ALTER COLUMN chunker_config_id DROP NOT NULL;

-- Exactly one path must be set
ALTER TABLE vector_set ADD CONSTRAINT chk_vector_set_type
    CHECK (
        (chunker_config_id IS NOT NULL AND semantic_config_id IS NULL AND granularity IS NULL)
        OR
        (chunker_config_id IS NULL AND semantic_config_id IS NOT NULL AND granularity IS NOT NULL)
    );
```

### Granularity Values

Enum in proto, string in DB:

| Granularity | Description | Embedding source |
|---|---|---|
| `SEMANTIC_CHUNK` | Topic-boundary grouped chunks | Same model, re-embedded |
| `SENTENCE` | Raw sentence vectors from boundary detection | Same model, already computed |
| `PARAGRAPH` | Averaged sentence vectors per paragraph | Computed, zero cost |
| `SECTION` | Averaged sentence vectors per heading section | Computed, zero cost |
| `DOCUMENT` | Single averaged vector for entire document | Computed, zero cost |

### Entity Relationship

```
                              vector_set
                             +------------------------+
chunker_config ──(nullable)──| chunker_config_id      |
                             | semantic_config_id     |──(nullable)── semantic_config
                             | granularity            |                    |
embedding_model_config ──────| embedding_model_config_id |               embedding_model_config
                             +------------------------+
                                      |
                             vector_set_index_binding → index
```

Standard VectorSet: `chunker_config_id` set, `semantic_config_id` null.
Semantic VectorSet: `semantic_config_id` + `granularity` set, `chunker_config_id` null.

### Registration Flow

When `CreateSemanticConfig` is called:

1. Validate the embedding model exists
2. Persist `semantic_config` row
3. Create child VectorSets based on flags:
   - Always: `SEMANTIC_CHUNK` VectorSet
   - If `store_sentence_vectors=true`: `SENTENCE` VectorSet
   - If `compute_centroids=true`: `PARAGRAPH`, `DOCUMENT` VectorSets
   - `SECTION` created when `compute_centroids=true` (populated only when doc outline exists)
4. Return the semantic config + created VectorSet IDs

All VectorSets reference the same `embedding_model_config_id` from the semantic config. All share `source_cel` from the directive that triggered registration.

### Document Flow (Indexing)

When opensearch-manager receives an `OpenSearchDocument` with `SemanticVectorSet` entries:

1. Check if `semantic_config_id` is set on the `SemanticVectorSet`
2. If yes: look up VectorSet by `(semantic_config_id, granularity)` — pure lookup, no creation
3. Create index binding if needed (first document for this index)
4. Ensure OpenSearch nested KNN mapping exists
5. Index the vectors

No `resolveChunkerConfig`, no `resolveOrCreateVectorSet`, no races.

## Proto Changes

### opensearch_document.proto — SemanticVectorSet

Add semantic identification fields:

```protobuf
message SemanticVectorSet {
  string source_field_name = 1;
  string chunk_config_id = 2;      // Standard path (existing)
  string embedding_id = 3;
  repeated OpenSearchEmbedding embeddings = 4;
  optional string vector_set_id = 5;
  optional string nested_field_name = 6;

  // Semantic path (new)
  optional string semantic_config_id = 7;
  optional SemanticGranularity granularity = 8;
}

enum SemanticGranularity {
  SEMANTIC_GRANULARITY_UNSPECIFIED = 0;
  SEMANTIC_GRANULARITY_SEMANTIC_CHUNK = 1;
  SEMANTIC_GRANULARITY_SENTENCE = 2;
  SEMANTIC_GRANULARITY_PARAGRAPH = 3;
  SEMANTIC_GRANULARITY_SECTION = 4;
  SEMANTIC_GRANULARITY_DOCUMENT = 5;
}
```

### opensearch_manager_service.proto — New RPCs

```protobuf
rpc CreateSemanticConfig(CreateSemanticConfigRequest) returns (CreateSemanticConfigResponse);
rpc GetSemanticConfig(GetSemanticConfigRequest) returns (GetSemanticConfigResponse);
rpc ListSemanticConfigs(ListSemanticConfigsRequest) returns (ListSemanticConfigsResponse);
rpc DeleteSemanticConfig(DeleteSemanticConfigRequest) returns (DeleteSemanticConfigResponse);
```

### pipeline_core_types.proto — SemanticProcessingResult

Add semantic config reference so the opensearch-sink can propagate it:

```protobuf
message SemanticProcessingResult {
  // ... existing fields ...
  optional string semantic_config_id = 10;
  optional SemanticGranularity granularity = 11;
}
```

## Affected Repos

| Repo | Changes |
|---|---|
| pipestream-protos | SemanticGranularity enum, SemanticVectorSet fields, SemanticProcessingResult fields, CreateSemanticConfig RPCs |
| pipestream-opensearch (opensearch-manager) | New entity, Flyway migration, CRUD service, resolution path in OpenSearchIndexingService |
| module-semantic-manager | Stamp outputs with semantic_config_id + granularity instead of string chunk_config_ids |
| module-opensearch-sink | Propagate semantic_config_id + granularity from SemanticProcessingResult to SemanticVectorSet |
| module-testing-sidecar | Register semantic config during E2E setup, pass semantic_config_id in graph directives |

## What This Fixes

1. "Chunker config not found: sentence" — semantic results no longer go through chunker config resolution
2. Race conditions on VectorSet creation during document flow — all VectorSets pre-exist
3. Conceptual mismatch — granularity labels are a proper enum, not overloaded chunk_config_ids

## Directive and Pipeline Config — What Changes vs What Doesn't

### Doesn't change

The directive structure (`DirectiveConfig`) is untouched:

```json
{
  "source_label": "body",
  "cel_selector": "document.search_metadata.body",
  "chunker_configs": [{"config_id": "default", "config": {"algorithm": "TOKEN", ...}}],
  "embedder_configs": [{"config_id": "all-MiniLM-L6-v2"}]
}
```

This continues to produce M x N standard VectorSets. The DAG structure, source/destination
wiring, and directive cartesian product are all unchanged.

### What changes

`SemanticManagerOptions` (the pipeline node's JSON config) gets one new optional field:

```json
{
  "index_name": "my-index",
  "directives": [ ... ],
  "semantic_config_id": "my-semantic-config"
}
```

That's it. The semantic manager receives this ID, uses the pre-registered parameters
(thresholds, flags, embedding model) from opensearch-manager's DB, runs the semantic
algorithm, and stamps each output with the ID + granularity.

The semantic path is a parallel process triggered by the presence of `semantic_config_id`,
not a modification to the directive structure. Standard M x N and semantic coexist.

### Lifecycle

1. Admin registers chunker configs, embedding configs (existing)
2. Admin registers a `semantic_config` referencing an embedding model (new) — child VectorSets created immediately
3. Pipeline graph node config for semantic-manager includes `semantic_config_id`
4. Documents flow — standard path uses directives (M x N), semantic path uses the pre-registered config
5. At indexing time, all VectorSets already exist. Pure lookups, no creation, no races.

Adding a second semantic config with a different model to the same index later is just:
register new config → VectorSets created → update pipeline node config. No schema migration,
no index rebuild.

## What Doesn't Change

1. Standard M x N chunking path — untouched
2. Existing VectorSet, ChunkerConfig, EmbeddingModelConfig entities — unchanged
3. Directive structure (`DirectiveConfig`) — unchanged
4. OpenSearch nested mapping creation — same pattern, just triggered by semantic VectorSets too
5. The semantic manager's internal algorithm (boundary detection, centroid computation) — unchanged
6. Pipeline DAG structure, source/destination wiring — unchanged

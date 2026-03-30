# VectorSet Phase 1 — Implementation Plan

## Locked Decisions

1. **VectorSet is the canonical authority** for index-to-model mapping. `IndexEmbeddingBinding` becomes compatibility/read-path only during transition.
2. **Sink stays stateless** — no lifecycle knobs (`ensureOnStart`, `allowSchemaUpdate`, etc.) in this phase.
3. **Ensure-on-write remains mandatory** — no eager provisioning in Phase 1.
4. **`embeddings_<dim>` strategy is the stable baseline** — no strategy variants until VectorSet and compat are stable.
5. **opensearch-manager owns all index lifecycle and mapping invariants.** module-opensearch-sink is a stateless gRPC client.

---

## Ownership Split (Explicit)

| Component | Owns | Does NOT own |
|-----------|------|-------------|
| **opensearch-manager** | VectorSet entity, ChunkerConfig, EmbeddingModelConfig, IndexEmbeddingBinding (legacy), index schema creation/update, field naming (`embeddings_<dim>`), dimension resolution, in-use constraint enforcement, Kafka semantic events | Document writes, pipeline routing |
| **module-opensearch-sink** | PipeDoc-to-OpenSearchDocument conversion, calling `ensureNestedEmbeddingsFieldExists` before writing, writing documents to OpenSearch via bulk gRPC | Schema logic, dimension resolution, field naming, config CRUD |
| **module-chunker** | Chunking text per config JSON, returning `SemanticProcessingResult` with `chunkConfigId = pipeStepName` | Config storage, schema |
| **module-embedder** | Embedding chunks per config JSON, returning vectors with `embeddingConfigId = pipeStepName` | Config storage, schema |
| **Engine** | DAG execution, config hydration (inline JSON), passing `pipeStepName` in metadata | Semantic config interpretation |

---

## Current State: What Exists

| Entity | Table | Proto | gRPC Service | Status |
|--------|-------|-------|-------------|--------|
| EmbeddingModelConfig | `embedding_model_config` | `embedding_config.proto` | `EmbeddingConfigService` | Done, tested |
| IndexEmbeddingBinding | `index_embedding_binding` | `embedding_config.proto` | `EmbeddingConfigService` | Done, tested |
| ChunkerConfigEntity | `chunker_config` | `chunker_config.proto` | `ChunkerConfigService` | Done, tested |
| **VectorSet** | — | Draft in README | — | **Not started** |

### Current Field Name Discrepancy (Pre-existing)

- **opensearch-manager's `indexDocument` path** uses `"embeddings_" + dim` (e.g., `embeddings_384`, `embeddings_768`).
- **module-opensearch-sink's `SchemaManagerService`** hardcodes `"embeddings"` as the nested field name.
- **Impact on Phase 1:** VectorSet will use the opensearch-manager convention (`embeddings_<dim>`). The sink discrepancy is a pre-existing issue and is out of scope for Phase 1 but should be noted as a follow-up.

---

## VectorSet Entity Design

### Proto: `vector_set.proto`

New file: `pipestream-protos/opensearch/proto/ai/pipestream/opensearch/v1/vector_set.proto`

```protobuf
syntax = "proto3";
package ai.pipestream.opensearch.v1;

import "google/protobuf/timestamp.proto";
import "google/protobuf/struct.proto";

// VectorSet is the canonical authority for the semantic tuple:
// (chunker config + embedding model + index + field + result set).
// It defines what vector configuration is valid for a given index/field
// and replaces IndexEmbeddingBinding as the source of truth.
message VectorSet {
  string id = 1;
  string name = 2;                           // Human-readable, unique
  string chunker_config_id = 3;              // FK to ChunkerConfig
  string embedding_model_config_id = 4;      // FK to EmbeddingModelConfig
  string index_name = 5;                     // Target OpenSearch index
  string field_name = 6;                     // Nested field name (e.g. "embeddings")
  string result_set_name = 7;               // e.g. "default", "title-only"
  string source_field = 8;                   // Source text field (e.g. "body", "title")
  int32 vector_dimensions = 9;              // Denormalized from EmbeddingModelConfig.dimensions
  google.protobuf.Timestamp created_at = 10;
  google.protobuf.Timestamp updated_at = 11;
  google.protobuf.Struct metadata = 12;      // Extensible JSONB
}

service VectorSetService {
  // CRUD
  rpc CreateVectorSet(CreateVectorSetRequest) returns (CreateVectorSetResponse);
  rpc GetVectorSet(GetVectorSetRequest) returns (GetVectorSetResponse);
  rpc UpdateVectorSet(UpdateVectorSetRequest) returns (UpdateVectorSetResponse);
  rpc DeleteVectorSet(DeleteVectorSetRequest) returns (DeleteVectorSetResponse);
  rpc ListVectorSets(ListVectorSetsRequest) returns (ListVectorSetsResponse);

  // Resolve: given (index_name, field_name, result_set_name), return the active VectorSet
  rpc ResolveVectorSet(ResolveVectorSetRequest) returns (ResolveVectorSetResponse);
}

// --- Create ---
message CreateVectorSetRequest {
  string id = 1;                             // Optional; server generates if empty
  string name = 2;
  string chunker_config_id = 3;             // Required: must exist
  string embedding_model_config_id = 4;     // Required: must exist
  string index_name = 5;                    // Required
  string field_name = 6;                    // Required
  string result_set_name = 7;              // Defaults to "default"
  string source_field = 8;                 // Required
  google.protobuf.Struct metadata = 9;
}

message CreateVectorSetResponse {
  VectorSet vector_set = 1;
}

// --- Get ---
message GetVectorSetRequest {
  oneof lookup {
    string id = 1;
    string name = 2;
  }
}

message GetVectorSetResponse {
  VectorSet vector_set = 1;
}

// --- Update ---
message UpdateVectorSetRequest {
  string id = 1;                            // Required
  optional string name = 2;
  optional string chunker_config_id = 3;
  optional string embedding_model_config_id = 4;
  optional string source_field = 5;
  optional string result_set_name = 6;
  google.protobuf.Struct metadata = 7;
}

message UpdateVectorSetResponse {
  VectorSet vector_set = 1;
}

// --- Delete ---
message DeleteVectorSetRequest {
  string id = 1;
}

message DeleteVectorSetResponse {
  bool success = 1;
  string message = 2;
}

// --- List ---
message ListVectorSetsRequest {
  string index_name = 1;                   // Optional filter
  string chunker_config_id = 2;           // Optional filter
  string embedding_model_config_id = 3;   // Optional filter
  int32 page_size = 4;
  string page_token = 5;
}

message ListVectorSetsResponse {
  repeated VectorSet vector_sets = 1;
  string next_page_token = 2;
}

// --- Resolve ---
message ResolveVectorSetRequest {
  string index_name = 1;                   // Required
  string field_name = 2;                   // Required
  string result_set_name = 3;             // Defaults to "default" if empty
}

message ResolveVectorSetResponse {
  VectorSet vector_set = 1;               // null/empty if not found
  bool found = 2;
}
```

### Uniqueness Rule

Composite unique constraint: `(index_name, field_name, result_set_name)`

This means one VectorSet per (index, field, result_set) — which is the same cardinality as `IndexEmbeddingBinding` today, but with the chunker config and source field added.

**Rationale:** A given index/field/result_set can only be served by one chunker+embedder combination. If you want a different chunker, that's a different result_set.

### DB Migration: `V4__create_vector_set_table.sql`

```sql
-- VectorSet: canonical authority for semantic tuple
-- (chunker_config + embedding_model + index + field + result_set)
CREATE TABLE vector_set (
    id                          VARCHAR(255) PRIMARY KEY,
    name                        VARCHAR(255) NOT NULL,
    chunker_config_id           VARCHAR(255) NOT NULL,
    embedding_model_config_id   VARCHAR(255) NOT NULL,
    index_name                  VARCHAR(255) NOT NULL,
    field_name                  VARCHAR(255) NOT NULL,
    result_set_name             VARCHAR(255) NOT NULL DEFAULT 'default',
    source_field                VARCHAR(255) NOT NULL,
    vector_dimensions           INTEGER,
    metadata                    JSONB,
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_vector_set_name UNIQUE(name),
    CONSTRAINT unique_vector_set_index_field_result_set
        UNIQUE(index_name, field_name, result_set_name),

    CONSTRAINT fk_vector_set_chunker_config
        FOREIGN KEY (chunker_config_id) REFERENCES chunker_config(id),
    CONSTRAINT fk_vector_set_embedding_model_config
        FOREIGN KEY (embedding_model_config_id) REFERENCES embedding_model_config(id)
);

CREATE INDEX idx_vector_set_index_name ON vector_set(index_name);
CREATE INDEX idx_vector_set_chunker_config_id ON vector_set(chunker_config_id);
CREATE INDEX idx_vector_set_embedding_model_config_id ON vector_set(embedding_model_config_id);
CREATE INDEX idx_vector_set_created_at ON vector_set(created_at DESC);
```

**Note:** No `ON DELETE CASCADE` on FKs — deletion of referenced configs must be blocked (in-use protection).

### JPA Entity: `VectorSetEntity.java`

New file: `src/main/java/ai/pipestream/schemamanager/entity/VectorSetEntity.java`

Following the existing Panache active-record pattern:

- Extends `PanacheEntityBase`
- String UUID ID (assigned in service layer)
- `@ManyToOne(fetch = FetchType.EAGER)` for both `chunkerConfig` and `embeddingModelConfig`
- JSONB `metadata` field
- `@CreationTimestamp` / `@UpdateTimestamp`
- Unique constraint: `(index_name, field_name, result_set_name)` at `@Table` level
- Static finder methods:
  - `findByName(String name)`
  - `findByIndexFieldAndResultSetName(String indexName, String fieldName, String resultSetName)`
  - `findByChunkerConfigId(String chunkerConfigId)` — for in-use checks
  - `findByEmbeddingModelConfigId(String embeddingModelConfigId)` — for in-use checks
  - `listByIndexName(String indexName)`
  - `listOrderedByCreatedDesc(int page, int pageSize)`

---

## gRPC Service: `VectorSetServiceImpl.java`

New file: `src/main/java/ai/pipestream/schemamanager/VectorSetServiceImpl.java`

Follows the existing `@GrpcService` pattern. Key behaviors:

### Create
1. Validate `chunker_config_id` exists (query ChunkerConfigEntity)
2. Validate `embedding_model_config_id` exists (query EmbeddingModelConfig)
3. Denormalize `vector_dimensions` from `embeddingModelConfig.dimensions`
4. Default `result_set_name` to `"default"` if empty
5. Persist entity
6. Publish `VECTOR_SET_CREATED` event
7. Return response

### Update
1. Find entity by ID (NOT_FOUND if missing)
2. Snapshot previous state for event
3. If `chunker_config_id` changed, validate new one exists
4. If `embedding_model_config_id` changed, validate and re-denormalize `vector_dimensions`
5. Persist
6. Publish `VECTOR_SET_UPDATED` event (previous + current)

### Delete
1. Find entity by ID
2. Delete (no cascading — VectorSet has no dependents in Phase 1)
3. Publish `VECTOR_SET_DELETED` event

### Resolve
1. Query `VectorSetEntity.findByIndexFieldAndResultSetName(index, field, resultSet)`
2. If not found and resultSet != "default", fallback to resultSet = "default"
3. Return `found: true/false` + entity if found

---

## In-Use Protection

### ChunkerConfig delete: block if referenced by VectorSet

Update `ChunkerConfigServiceImpl.deleteChunkerConfig()`:

Before deleting, query `VectorSetEntity.findByChunkerConfigId(configId)`. If any exist, return `FAILED_PRECONDITION` error:
```
"Cannot delete chunker config '%s': referenced by %d VectorSet(s)"
```

This fulfills the contract already documented in `chunker_config.proto` line 59: "This operation will fail if the config is referenced by any VectorSet."

### EmbeddingModelConfig delete: block if referenced by VectorSet

Update `EmbeddingConfigServiceImpl.deleteEmbeddingModelConfig()`:

Before deleting, query `VectorSetEntity.findByEmbeddingModelConfigId(configId)`. If any exist, return `FAILED_PRECONDITION`:
```
"Cannot delete embedding model config '%s': referenced by %d VectorSet(s)"
```

**Note:** `IndexEmbeddingBinding` already has `ON DELETE CASCADE` from `EmbeddingModelConfig`. That behavior stays for backward compat. VectorSet does NOT cascade — it blocks.

---

## Kafka Events

### Update `semantic_metadata_events.proto`

Add 3 new event types to `SemanticMetadataEventType`:

```protobuf
SEMANTIC_METADATA_EVENT_TYPE_VECTOR_SET_CREATED = 10;
SEMANTIC_METADATA_EVENT_TYPE_VECTOR_SET_UPDATED = 11;
SEMANTIC_METADATA_EVENT_TYPE_VECTOR_SET_DELETED = 12;
```

Add payload fields to `SemanticMetadataEvent`:

```protobuf
VectorSet vector_set = 7;                    // in the payload oneof
VectorSet previous_vector_set = 8;           // in the previous_payload oneof
```

### Update `SemanticMetadataEventProducer.java`

Add three methods following the existing pattern:
- `publishVectorSetCreated(VectorSet vs)`
- `publishVectorSetUpdated(VectorSet previous, VectorSet current)`
- `publishVectorSetDeleted(String entityId)`

---

## EmbeddingBindingResolver: VectorSet-Preferred Resolution

Update `EmbeddingBindingResolver.java` to prefer VectorSet over IndexEmbeddingBinding:

```
resolve(indexName, fieldName, resultSetName):
  1. Query VectorSetEntity.findByIndexFieldAndResultSetName(index, field, resultSet)
  2. If found → return VectorFieldDefinition with vs.vectorDimensions
  3. If not found and resultSet != "default" → try resultSet = "default" via VectorSet
  4. If still not found → fall back to IndexEmbeddingBinding lookup (existing logic)
  5. If nothing → return null (caller fails with actionable error)
```

This maintains backward compatibility: existing `IndexEmbeddingBinding` records still work, but VectorSet takes priority.

---

## IndexEmbeddingBinding: Backward Compatibility

No changes to `IndexEmbeddingBinding` entity, service, or proto in Phase 1. It continues to work as-is. The only change is that `EmbeddingBindingResolver` now checks VectorSet first.

**Migration path (future phases):**
- Phase 2: Add optional `vector_set_id` FK to `IndexEmbeddingBinding` for traceability
- Phase 3: Deprecate direct `IndexEmbeddingBinding` creation; auto-create from VectorSet

---

## Tests

### New: `VectorSetServiceGrpcTest.java`

Following the existing `EmbeddingConfigServiceGrpcTest` pattern:

1. **CRUD lifecycle:**
   - Create with all fields → verify response
   - Create with server-generated ID → verify UUID format
   - Create with duplicate `(index_name, field_name, result_set_name)` → verify ALREADY_EXISTS
   - Create with duplicate `name` → verify ALREADY_EXISTS
   - Get by ID → verify all fields
   - Get by name → verify all fields
   - Get non-existent → verify NOT_FOUND
   - Update name, source_field, metadata → verify updated
   - Update chunker_config_id → verify re-links, same dimensions
   - Update embedding_model_config_id → verify re-denormalized dimensions
   - Delete → verify success
   - Delete non-existent → verify success: false

2. **Resolve:**
   - Resolve existing (index, field, resultSet) → verify found: true, correct VectorSet
   - Resolve with fallback to "default" → verify found: true
   - Resolve non-existent → verify found: false

3. **In-use constraints:**
   - Create VectorSet referencing ChunkerConfig → try delete ChunkerConfig → verify FAILED_PRECONDITION
   - Create VectorSet referencing EmbeddingModelConfig → try delete EmbeddingModelConfig → verify FAILED_PRECONDITION
   - Delete VectorSet → then delete ChunkerConfig → verify success

4. **Dimension denormalization:**
   - Create VectorSet with embedding model that has dimensions=384 → verify vector_dimensions=384
   - Update VectorSet to different embedding model (dimensions=768) → verify vector_dimensions=768

5. **Kafka events (verify via test consumer or mock):**
   - Create → verify VECTOR_SET_CREATED event published
   - Update → verify VECTOR_SET_UPDATED event with previous + current
   - Delete → verify VECTOR_SET_DELETED event

### Update: `EmbeddingConfigServiceGrpcTest.java`

Add test cases:
- Delete EmbeddingModelConfig that is referenced by a VectorSet → verify FAILED_PRECONDITION

### New or Update: `ChunkerConfigServiceGrpcTest.java`

Add test cases:
- Delete ChunkerConfig that is referenced by a VectorSet → verify FAILED_PRECONDITION

### Update: `EmbeddingBindingResolver` tests (or `SchemaManagerServiceTest`)

Add test case:
- When both VectorSet and IndexEmbeddingBinding exist for same (index, field, resultSet), VectorSet wins

---

## Edge Cases and Error Behavior

| Scenario | Behavior |
|----------|----------|
| Create VectorSet with non-existent chunker_config_id | `NOT_FOUND`: "Chunker config not found: {id}" |
| Create VectorSet with non-existent embedding_model_config_id | `NOT_FOUND`: "Embedding model config not found: {id}" |
| Create VectorSet with duplicate (index, field, result_set) | `ALREADY_EXISTS`: "VectorSet already exists for index={}, field={}, resultSet={}" |
| Create VectorSet with embedding model that has null dimensions | Succeeds; `vector_dimensions` is 0/null. Schema creation will fail later at ensure-time (actionable error). |
| Update VectorSet index_name/field_name | **Not allowed in Phase 1.** These are part of the unique key. To change, delete and re-create. |
| Delete ChunkerConfig referenced by VectorSet | `FAILED_PRECONDITION` with count of referencing VectorSets |
| Delete EmbeddingModelConfig referenced by VectorSet | `FAILED_PRECONDITION` with count of referencing VectorSets |
| Resolve with empty result_set_name | Treat as "default" |
| Resolve: VectorSet exists but IndexEmbeddingBinding also exists | VectorSet wins |
| Resolve: No VectorSet, IndexEmbeddingBinding exists | IndexEmbeddingBinding used (backward compat) |
| Resolve: Neither exists | Return null; caller gets actionable error |
| Repeated ensure calls (idempotent) | No change; existing behavior preserved |

---

## Files to Create/Modify

### Create (4 files)
| File | Description |
|------|-------------|
| `pipestream-protos/opensearch/proto/ai/pipestream/opensearch/v1/vector_set.proto` | VectorSet message + VectorSetService gRPC |
| `opensearch-manager/src/main/resources/db/migration/V4__create_vector_set_table.sql` | Flyway migration |
| `opensearch-manager/src/main/java/ai/pipestream/schemamanager/entity/VectorSetEntity.java` | JPA entity |
| `opensearch-manager/src/main/java/ai/pipestream/schemamanager/VectorSetServiceImpl.java` | gRPC service |

### Modify (5 files)
| File | Change |
|------|--------|
| `pipestream-protos/opensearch/proto/ai/pipestream/opensearch/v1/semantic_metadata_events.proto` | Add VectorSet event types + payload fields |
| `opensearch-manager/src/main/java/ai/pipestream/schemamanager/kafka/SemanticMetadataEventProducer.java` | Add VectorSet event methods |
| `opensearch-manager/src/main/java/ai/pipestream/schemamanager/EmbeddingBindingResolver.java` | VectorSet-first resolution |
| `opensearch-manager/src/main/java/ai/pipestream/schemamanager/ChunkerConfigServiceImpl.java` | In-use check on delete |
| `opensearch-manager/src/main/java/ai/pipestream/schemamanager/EmbeddingConfigServiceImpl.java` | In-use check on delete |

### Test files (create or modify, 3 files)
| File | Change |
|------|--------|
| `opensearch-manager/src/test/.../VectorSetServiceGrpcTest.java` | **New:** Full CRUD + resolve + constraint tests |
| `opensearch-manager/src/test/.../EmbeddingConfigServiceGrpcTest.java` | Add in-use constraint test |
| `opensearch-manager/src/test/.../ChunkerConfigServiceGrpcTest.java` | Add in-use constraint test |

---

## Execution Order

1. **Proto first** — `vector_set.proto` + `semantic_metadata_events.proto` updates. Build protos to generate Java.
2. **Migration** — `V4__create_vector_set_table.sql`
3. **Entity** — `VectorSetEntity.java`
4. **Kafka events** — Update `SemanticMetadataEventProducer.java`
5. **Service** — `VectorSetServiceImpl.java` (CRUD + Resolve + event publishing)
6. **In-use checks** — Update `ChunkerConfigServiceImpl` and `EmbeddingConfigServiceImpl` delete methods
7. **Resolver** — Update `EmbeddingBindingResolver` to prefer VectorSet
8. **Tests** — `VectorSetServiceGrpcTest`, update existing tests for constraint behavior
9. **Docs** — Update `IMPLEMENTATION_PLAN.md` and `SEMANTIC_LAYER_END_TO_END_FOR_LLM.md`

---

## Out of Scope (Phase 1)

- Sink config formalization (`indexName`, `resultSetName` as explicit sink node config fields)
- Eager provisioning (ensure-on-graph-save)
- UI-driven index editing / reconciliation
- `vectorSchemaStrategy` variants (only `embeddings_<dim>`)
- IndexEmbeddingBinding deprecation or removal
- Fixing the sink's hardcoded `"embeddings"` vs manager's `"embeddings_<dim>"` field name discrepancy
- Auto-creating VectorSet from IndexEmbeddingBinding data

# Semantic Layer End-to-End Guide for LLM-Assisted Edits

This guide is optimized for fast edits and test planning. It captures the current source-of-truth flow in `opensearch-manager` and the cross-service architecture contracts used by the semantic stack.

## 1) What is the semantic ownership model right now

`opensearch-manager` is the owner of semantic metadata for indexing:

- **VectorSet** is the canonical authority for the full semantic tuple (chunker config + embedding model + index + field + result set). Managed by `VectorSetService`.
- Chunker configs are managed by `ChunkerConfigService`.
- Embedding model configs are managed by `EmbeddingConfigService`.
- Index → field binding is managed by `IndexEmbeddingBinding` (legacy, backward-compat read path; VectorSet takes priority in resolution).
- Field mappings / index mapping strategy for semantic fields are created/updated by `OpenSearchManagerService` + `OpenSearchSchemaServiceImpl`.

**Ownership split:**
- `opensearch-manager` owns all index lifecycle, mapping invariants, semantic config CRUD, VectorSet, and dimension resolution.
- `module-opensearch-sink` is stateless for pipeline execution; it only issues gRPC calls to `opensearch-manager` for schema and index operations.

The current practical contract remains:

- Modules receive hydrated config JSON from the engine (per-node config), no `config_id` required by the module contract.
- `opensearch-manager` interprets semantic metadata and owns how index field names are resolved and validated.
- Dimension resolution order: explicit `VectorFieldDefinition` on ensure → `resolve_vector_set_id` / inline config ids on `EnsureNestedEmbeddingsFieldExistsRequest` → VectorSet index binding (preferred) → IndexEmbeddingBinding (legacy fallback) → null (actionable error). See `docs/VECTOR_SET_RESOLUTION.md` and gRPC `ResolveVectorSetFromDirective`.

## 2) End-to-end document-to-index path

At a high level:

1. Document arrives through engine intake path and is routed by graph metadata.
2. Chunker/Embedder modules emit semantic outputs in the document model.
3. OpenSearch sink invokes `opensearch-manager` before/while indexing (indexing and ensure calls are in one service boundary).
4. `opensearch-manager` ensures required nested embedding fields exist for each vector dimension before indexing.
5. Indexing request is sent through OpenSearch gRPC first, with REST fallback if needed.
6. Document is committed in OpenSearch and can be queried via existing sink/index consumers.

## 3) gRPC API behavior to edit/test confidently

Primary service classes:

- `ai.pipestream.schemamanager.VectorSetServiceImpl` — VectorSet CRUD + Resolve
- `ai.pipestream.schemamanager.ChunkerConfigServiceImpl`
- `ai.pipestream.schemamanager.EmbeddingConfigServiceImpl`
- `ai.pipestream.schemamanager.OpenSearchManagerService`
- `ai.pipestream.schemamanager.EmbeddingBindingResolver` — VectorSet-preferred, IndexEmbeddingBinding fallback
- `ai.pipestream.schemamanager.opensearch.OpenSearchSchemaServiceImpl`

Key flows:

- `EnsureNestedEmbeddingsFieldExists(index_name, nested_field_name, vector_field_definition?)`
  - Resolves vector dimensions either from explicit definition or via `EmbeddingBindingResolver` (VectorSet first, then IndexEmbeddingBinding fallback).
  - If nested mapping already exists, returns success.
  - Otherwise creates index/mapping (or updates existing index mapping).
- `indexDocument(...)`
  - Computes unique vector dimensions from `OpenSearchDocument.embeddings`.
  - For each dimension, ensures nested field `embeddings_<dim>`.
  - Builds bulk request and writes through OpenSearch gRPC client; falls back to REST on gRPC client unavailability.

## 4) Semantic config entities and DB-backed resolution

### 4.0 `VectorSet` (canonical authority)

`vector_set` is unique on:

- `(index_name, field_name, result_set_name)` — one VectorSet per tuple
- `name` — human-readable unique name

Fields: `id`, `name`, `chunker_config_id` (FK), `embedding_model_config_id` (FK), `index_name`, `field_name`, `result_set_name`, `source_field`, `vector_dimensions` (denormalized from embedding config), `metadata` (JSONB), timestamps.

Resolution behavior (via `EmbeddingBindingResolver`):

1. VectorSet by `(index_name, field_name, result_set_name)` — exact match
2. VectorSet by `(index_name, field_name, "default")` — fallback
3. IndexEmbeddingBinding by `(index_name, field_name, result_set_name)` — legacy compat
4. IndexEmbeddingBinding by `(index_name, field_name, "default")` — legacy fallback
5. null → caller gets actionable error

In-use protection: deleting a ChunkerConfig or EmbeddingModelConfig that is referenced by any VectorSet returns `FAILED_PRECONDITION`.

### 4.1 `IndexEmbeddingBinding` (legacy, backward-compat)

`index_embedding_binding` is unique on:

- `index_name`
- `field_name`
- `result_set_name`

This entity is still functional but VectorSet takes priority in resolution. No changes to IndexEmbeddingBinding in Phase 1.

### 4.2 `EmbeddingModelConfig`

- Stores model metadata and dimension.
- Used to derive embedding dimension for mapping when lookup is done through VectorSet or IndexEmbeddingBinding.
- Delete blocked if referenced by any VectorSet.

### 4.3 `ChunkerConfig`

- Chunker config is CRUD-managed and stores normalized `config_id` if not provided (derived from config fields when possible).
- This service emits semantic metadata events so the rest of the platform can react to semantic config changes.
- Delete blocked if referenced by any VectorSet.

## 5) Mapping/field behavior you must preserve

The current default nested field names used by automatic schema creation are:

- `embeddings_<N>` where `N` is vector dimension.

Each `embeddings_<N>` field maps to nested objects with:

- `vector` (`knn_vector` with dimension + optional HNSW options)
- `source_text` (`text`)
- `context_text` (`text`)
- `chunk_config_id` (`keyword`)
- `embedding_id` (`keyword`)
- `is_primary` (`boolean`)

This means changes that alter vector dimensions or field naming impact mapping idempotency and should be coordinated through:

- existing binding updates
- index-specific test coverage
- index management checks

## 6) Testing path (what to run after edits)

Use these as the default loop:

1. VectorSet CRUD + constraint tests
   - `VectorSetServiceGrpcTest` (CRUD, resolve with fallback, in-use constraints, dimension denormalization)
2. Semantic metadata CRUD tests
   - `EmbeddingConfigServiceGrpcTest` (includes delete-blocked-by-VectorSet)
   - `ChunkerConfigServiceGrpcTest` (includes delete-blocked-by-VectorSet)
   - `EmbeddingConfigEntityTest`
3. OpenSearch schema + indexing tests
   - `SchemaManagerServiceTest`
   - `IndexAnyDocumentTest`
4. Focused behavior checks
   - create/update `IndexEmbeddingBinding` for different `result_set_name` values
   - verify `EnsureNestedEmbeddingsFieldExists` behavior for explicit vector definition and resolver-backed dimension
   - verify VectorSet-preferred resolution (VectorSet wins over IndexEmbeddingBinding)
   - verify gRPC bulk path remains primary, REST fallback only in controlled scenarios

## 7) Integration and discovery context

- `TelemetryResource` exposes non-transactional operational endpoints for fast verification:
  - `/internal/telemetry/version`
  - `/internal/telemetry/opensearch`
  - `/internal/telemetry/dynamic-grpc`
  - `/internal/telemetry/stack`
- `OpensearchConsulTestResource` and Kafka-consumer tests are already wired in current integration setup for real service checks.

## 8) Safe change checklist for LLM edits

When adding semantic behavior:

1. Define intended invariant in this doc + `docs/` first.
2. Trace call path from engine module output to `OpenSearchManagerService`.
3. Ensure gRPC contract and schema behavior align (especially `field_name`/`result_set_name`).
4. Add/update tests before changing resolver behavior.
5. Add a migration note if schema assumptions changed.

## 9) Canonical references (for fast context bootstrap)

- `pipestream-engine/docs/architecture/12-semantic-config-vectorset.md`
- `pipestream-engine/docs/architecture/PIPELINE_ARCHITECTURE.md`
- `pipestream-engine/docs/architecture/DESIGN_SESSION_SEMANTIC_CONFIG.md`
- `opensearch-manager/docs/VECTOR_SET_PHASE1_PLAN.md` — Phase 1 implementation plan
- `opensearch-manager/src/main/java/ai/pipestream/schemamanager/VectorSetServiceImpl.java`
- `opensearch-manager/src/main/java/ai/pipestream/schemamanager/OpenSearchManagerService.java`
- `opensearch-manager/src/main/java/ai/pipestream/schemamanager/EmbeddingBindingResolver.java`
- `opensearch-manager/src/main/java/ai/pipestream/schemamanager/EmbeddingConfigServiceImpl.java`
- `opensearch-manager/src/main/java/ai/pipestream/schemamanager/ChunkerConfigServiceImpl.java`
- `opensearch-manager/src/main/java/ai/pipestream/schemamanager/entity/VectorSetEntity.java`
- `opensearch-manager/src/main/java/ai/pipestream/schemamanager/entity/IndexEmbeddingBinding.java`
- `opensearch-manager/src/main/java/ai/pipestream/schemamanager/opensearch/OpenSearchSchemaServiceImpl.java`
- `pipestream-protos/opensearch/proto/ai/pipestream/opensearch/v1/vector_set.proto`

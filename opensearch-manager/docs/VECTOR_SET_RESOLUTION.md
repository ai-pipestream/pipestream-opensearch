# VectorSet resolution rules

This document locks the design decisions for **VectorSet identity**, **CEL source selectors**, and **inline vs registered** tuples. Apicurio Registry holds **protobuf schemas only**, not live catalog rows.

## Precedence: `vector_set_id` vs inline tuple

- **`vector_set_id` wins**: If a `VectorDirective` (or `ResolveVectorSetFromDirectiveRequest`) supplies a non-empty `vector_set_id`, the resolver loads that row from PostgreSQL and returns the registered `VectorSet`. Inline chunker / embedder / CEL overrides in the same message are **not** applied (they must be omitted for clarity).
- **Inline only**: If `vector_set_id` is absent, the resolver builds an **ephemeral** `VectorSet` protobuf from `InlineVectorSetSpec` (chunker + embedding model config IDs, field names, `source_cel`).
- **Conflict**: If both a non-empty `vector_set_id` and an `inline` spec are sent in the **same** `oneof`, the wire format prevents that. If higher-level callers merge payloads incorrectly, gRPC handlers should reject contradictory combinations with `INVALID_ARGUMENT`.

## Inline materialization (registry row from inline)

Default for this release: **(a) ephemeral only** — inline resolution does **not** auto-upsert a `vector_set` row. Optional `content_signature` exists in the schema for a future **(b) hash-based upsert** or an explicit **(c) promote** API.

## CEL source selector

- Persisted column: **`source_cel`** (PostgreSQL `TEXT`). Legacy rows were migrated from `source_field`; short names such as `body` remain valid where the evaluator treats them as legacy paths.
- **Max length**: Configurable via `vectorset.semantic.source-cel.max-chars` (default `1048576`). Create/update rejects longer values.
- **Sandbox**: Full CEL compile/check uses the same policies as the engine CEL environment when integrated; until then, length and non-blank checks apply on the server.

## Provenance

- **`REGISTERED`**: Rows created via `CreateVectorSet` (default).
- **`INLINE`**: Ephemeral protobuf from `InlineVectorSetSpec` (not persisted).
- **`MATERIALIZED`**: Reserved for future auto-upsert / promote flows.

## Ownership

Optional `owner_type` / `owner_id` on `vector_set` are informational for policy and auditing; enforcement is deferred to upstream auth services.

## Single resolution path (opensearch-manager)

1. **gRPC** `ResolveVectorSetFromDirective` on `VectorSetService` — preferred API for sinks and the semantic pipeline.
2. **Ensure nested embeddings** — `EnsureNestedEmbeddingsFieldExistsRequest` may supply `resolve_vector_set_id` and/or `resolve_chunker_config_id` + `resolve_embedding_model_config_id` to resolve dimensions without an index binding.
3. **EmbeddingBindingResolver** — continues to resolve `(index, field, result_set)` from `vector_set_index_binding` with legacy fallback.

See also the Mermaid overview in the product plan (PipeDoc `VectorDirective` → `VectorSetResolver` → chunker / embedding / `vector_set`).

# opensearch-manager / Semantic Metadata Service — Implementation Plan

This document tracks the implementation steps for the semantic metadata service (opensearch-manager → intelligence-manager), including ChunkerConfig, VectorSet, and related features.

## Implementation Steps

### 1. ChunkerConfigService implementation
- [ ] Create Flyway migration for `chunker_config` table
- [ ] Implement Panache entity `ChunkerConfigEntity` (reactive)
- [ ] Implement repository/service layer
- [ ] Wire up gRPC `ChunkerConfigService` implementation
- [ ] Add grpcurl scripts for ChunkerConfig CRUD

### 2. VectorSet / Semantic Registry — Phase 1 (IN PROGRESS)
- [x] Proto definition: `vector_set.proto` (VectorSet message + VectorSetService gRPC with CRUD + Resolve)
- [x] Semantic metadata events: added VECTOR_SET_CREATED/UPDATED/DELETED to `semantic_metadata_events.proto`
- [x] DB migration: `V4__create_vector_set_table.sql` (FKs to chunker_config + embedding_model_config, no CASCADE)
- [x] JPA entity: `VectorSetEntity.java` (Panache active-record, ManyToOne eager, static finders)
- [x] gRPC service: `VectorSetServiceImpl.java` (CRUD + Resolve with "default" fallback)
- [x] Kafka events: added VectorSet publish methods to `SemanticMetadataEventProducer`
- [x] In-use protection: ChunkerConfig and EmbeddingModelConfig delete blocked when referenced by VectorSet
- [x] Resolver update: `EmbeddingBindingResolver` prefers VectorSet over IndexEmbeddingBinding
- [x] Tests: `VectorSetServiceGrpcTest` (CRUD, resolve, in-use constraints, dimension denormalization)
- [ ] buf lint + push (deferred — BSR was unreachable during implementation)
- [ ] Evolve IndexEmbeddingBinding to reference VectorSet (Phase 2)
- [ ] Deprecate direct IndexEmbeddingBinding creation (Phase 3)

### 3. CRUD tests for existing entities
- [x] EmbeddingModelConfig: Create, Get (by ID and name), Update, Delete, List — **EmbeddingConfigServiceGrpcTest** + **EmbeddingConfigEntityTest**
- [x] IndexEmbeddingBinding: Create, Get (by ID, by index+field), List, Update, Delete — **EmbeddingConfigServiceGrpcTest** + **EmbeddingConfigEntityTest**
- [ ] **Step 3.5 — Create CRUD tests for all entities**
  - Ensure full CRUD coverage for EmbeddingModelConfig (Create, Get, Update, Delete, List)
  - Ensure full CRUD coverage for IndexEmbeddingBinding (Create, Get, Update, Delete, List by index)
  - Add ChunkerConfigServiceGrpcTest (Create, Get, Update, Delete, List) once ChunkerConfigService is implemented
  - Add ChunkerConfig entity test (Panache CRUD) once entity exists

### 4. OpenSearch integration and wiremock
- [ ] Verify OpenSearch gRPC client (DocumentService bulk) in module-opensearch-sink
- [ ] **Step 4.5 — Pipestream-WireMock dummy data and test**
  - Add EmbeddingConfigServiceMock (and optionally ChunkerConfigServiceMock) to pipestream-wiremock-server with predefined dummy data (e.g. 2–3 embedding configs, 2–3 bindings)
  - Register mock in ServiceMockInitializer (META-INF/services)
  - Add integration test (in opensearch-manager or pipestream-test-support) that runs against the WireMock container and asserts on dummy data (ListEmbeddingModelConfigs returns mock data, Get by ID returns expected config)

### 5. Rename and packaging (optional)
- [ ] Rename service to intelligence-manager
- [ ] Consider package rename `ai.pipestream.opensearch.v1` → `ai.pipestream.intelligence.v1` (with compatibility)

---

## Test and WireMock Summary

| Step | Description |
|------|-------------|
| **3.5** | CRUD tests for all entities: EmbeddingModelConfig, IndexEmbeddingBinding, ChunkerConfig (when implemented). Both gRPC tests and entity-level (Panache) tests. |
| **4.5** | Pipestream-WireMock: dummy data for EmbeddingConfigService (and ChunkerConfigService when added); integration test that uses WireMock container and verifies mock responses. |

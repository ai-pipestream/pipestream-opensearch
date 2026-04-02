# Semantic Config Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `semantic_config` table and VectorSet granularity support so semantic chunking results (sentence, paragraph, document vectors) resolve through pre-registered configs instead of failing on non-existent chunker config lookups.

**Architecture:** New `semantic_config` entity in opensearch-manager with FK to embedding model. VectorSet gets nullable `semantic_config_id` + `granularity` columns (CHECK constraint: exactly one of chunker or semantic path). Registration creates child VectorSets eagerly. Document flow is pure lookup — no creation, no races. Proto gets `SemanticGranularity` enum propagated through `SemanticProcessingResult` → `SemanticVectorSet`.

**Tech Stack:** Java 21, Quarkus 3.x, Hibernate Reactive/Panache, Flyway, Protobuf, gRPC, Mutiny

---

## File Structure

### pipestream-protos (proto definitions)

| File | Action | Responsibility |
|------|--------|----------------|
| `opensearch/proto/.../opensearch_document.proto` | Modify | Add `SemanticGranularity` enum, new fields on `SemanticVectorSet` |
| `opensearch/proto/.../semantic_config.proto` | Create | New `SemanticConfigService` gRPC service + messages |
| `opensearch/proto/.../vector_set.proto` | Modify | Add optional `semantic_config_id` + `granularity` on `VectorSet` and `CreateVectorSetRequest` |
| `common/proto/.../pipeline_core_types.proto` | Modify | Add `semantic_config_id` + `granularity` on `SemanticProcessingResult` |

### pipestream-opensearch/opensearch-manager (DB + service layer)

| File | Action | Responsibility |
|------|--------|----------------|
| `db/migration/V8__create_semantic_config_table.sql` | Create | Flyway: new table + vector_set alterations |
| `entity/SemanticConfigEntity.java` | Create | JPA entity for `semantic_config` table |
| `entity/VectorSetEntity.java` | Modify | Make `chunkerConfig` nullable, add `semanticConfig` + `granularity` |
| `SemanticConfigServiceEngine.java` | Create | Business logic: CRUD + child VectorSet creation |
| `SemanticConfigServiceImpl.java` | Create | Thin gRPC wrapper delegating to engine |
| `OpenSearchIndexingService.java` | Modify | Add semantic resolution path in `resolveVectorSetsForDocument` |

### module-semantic-manager (stamp outputs)

| File | Action | Responsibility |
|------|--------|----------------|
| `config/SemanticManagerOptions.java` | Modify | Add `semantic_config_id` field |
| `service/SemanticIndexingOrchestrator.java` | Modify | Thread `semantic_config_id` + granularity through to all outputs |

### module-opensearch-sink (propagate fields)

| File | Action | Responsibility |
|------|--------|----------------|
| `service/DocumentConverterService.java` | Modify | Propagate `semantic_config_id` + `granularity` from result to vector set |

### module-testing-sidecar (E2E wiring)

| File | Action | Responsibility |
|------|--------|----------------|
| `e2e/E2EPipelineTestService.java` | Modify | Register semantic config during setup, pass ID through |
| `e2e/SemanticChunkingDirectiveBuilder.java` | Modify | Accept and propagate `semantic_config_id` |

---

## Task 1: Proto — SemanticGranularity enum and SemanticVectorSet fields

**Files:**
- Modify: `/work/core-services/pipestream-protos/opensearch/proto/ai/pipestream/opensearch/v1/opensearch_document.proto`

- [ ] **Step 1: Add SemanticGranularity enum and new fields to SemanticVectorSet**

Add after the `SemanticVectorSet` message closing brace (after line 83), add the enum. Then add two new fields to `SemanticVectorSet`:

```protobuf
// At the TOP of the file, after existing imports (or at end of file before closing):

// Granularity of vectors within a semantic vector set.
// Determines how vectors were produced and what they represent.
enum SemanticGranularity {
  SEMANTIC_GRANULARITY_UNSPECIFIED = 0;
  // Topic-boundary grouped chunks, re-embedded.
  SEMANTIC_GRANULARITY_SEMANTIC_CHUNK = 1;
  // Raw sentence vectors from boundary detection pass.
  SEMANTIC_GRANULARITY_SENTENCE = 2;
  // Averaged sentence vectors per paragraph.
  SEMANTIC_GRANULARITY_PARAGRAPH = 3;
  // Averaged sentence vectors per heading section.
  SEMANTIC_GRANULARITY_SECTION = 4;
  // Single averaged vector for entire document.
  SEMANTIC_GRANULARITY_DOCUMENT = 5;
}
```

Add two new fields to the `SemanticVectorSet` message:

```protobuf
message SemanticVectorSet {
  // ... existing fields 1-6 ...

  // Semantic path: references a pre-registered semantic config.
  // When set, opensearch-manager resolves VectorSet by (semantic_config_id, granularity)
  // instead of looking up chunk_config_id as a ChunkerConfig.
  optional string semantic_config_id = 7;

  // Granularity of vectors in this set (semantic path only).
  optional SemanticGranularity granularity = 8;
}
```

- [ ] **Step 2: Build protos to verify**

Run:
```bash
cd /work/core-services/pipestream-protos && ./gradlew :opensearch:build
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
cd /work/core-services/pipestream-protos
git add opensearch/proto/ai/pipestream/opensearch/v1/opensearch_document.proto
git commit -m "proto: add SemanticGranularity enum and fields to SemanticVectorSet"
```

---

## Task 2: Proto — SemanticProcessingResult fields

**Files:**
- Modify: `/work/core-services/pipestream-protos/common/proto/ai/pipestream/data/v1/pipeline_core_types.proto`

- [ ] **Step 1: Add granularity import and fields to SemanticProcessingResult**

Add import at top of file:

```protobuf
import "ai/pipestream/opensearch/v1/opensearch_document.proto";
```

Add two new fields to `SemanticProcessingResult` (after field 9, `centroid_metadata`):

```protobuf
message SemanticProcessingResult {
  // ... existing fields 1-9 ...

  // Semantic config reference — present when this result was produced by semantic chunking.
  // Propagated to SemanticVectorSet.semantic_config_id by the opensearch-sink.
  optional string semantic_config_id = 10;

  // Granularity of this result set (semantic path only).
  optional ai.pipestream.opensearch.v1.SemanticGranularity granularity = 11;
}
```

Note: If the import causes a circular dependency (common depends on opensearch), define the enum in `pipeline_core_types.proto` instead and import it FROM opensearch_document.proto. Check after building.

- [ ] **Step 2: Build protos to verify**

Run:
```bash
cd /work/core-services/pipestream-protos && ./gradlew build
```

If circular dependency error: move `SemanticGranularity` enum definition to `pipeline_core_types.proto` and import from there in `opensearch_document.proto`. The enum belongs wherever avoids the cycle.

Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
cd /work/core-services/pipestream-protos
git add common/proto/ai/pipestream/data/v1/pipeline_core_types.proto
git commit -m "proto: add semantic_config_id and granularity to SemanticProcessingResult"
```

---

## Task 3: Proto — SemanticConfigService and messages

**Files:**
- Create: `/work/core-services/pipestream-protos/opensearch/proto/ai/pipestream/opensearch/v1/semantic_config.proto`

- [ ] **Step 1: Create the semantic_config.proto file**

Follow the exact pattern from `chunker_config.proto`:

```protobuf
syntax = "proto3";

package ai.pipestream.opensearch.v1;

import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";
import "ai/pipestream/opensearch/v1/opensearch_document.proto";

option java_multiple_files = true;
option java_package = "ai.pipestream.opensearch.v1";

// SemanticConfigService — Semantic Chunking Configuration Registry API
//
// Manages semantic chunking configurations. A semantic config defines how
// sentence-level embedding similarity is used for topic boundary detection,
// what granularities to produce (sentence, paragraph, section, document),
// and which embedding model to use for all vector types.
//
// On creation, child VectorSets are eagerly created for each enabled
// granularity. Document flow resolves these by (semantic_config_id, granularity).
service SemanticConfigService {
  rpc CreateSemanticConfig(CreateSemanticConfigRequest) returns (CreateSemanticConfigResponse);
  rpc GetSemanticConfig(GetSemanticConfigRequest) returns (GetSemanticConfigResponse);
  rpc ListSemanticConfigs(ListSemanticConfigsRequest) returns (ListSemanticConfigsResponse);
  rpc DeleteSemanticConfig(DeleteSemanticConfigRequest) returns (DeleteSemanticConfigResponse);
}

// Stored semantic chunking configuration.
message SemanticConfig {
  string id = 1;
  string name = 2;
  string config_id = 3;

  // FK to EmbeddingModelConfig — single model for all granularities.
  string embedding_model_id = 4;

  // Boundary detection parameters.
  float similarity_threshold = 5;
  int32 percentile_threshold = 6;
  int32 min_chunk_sentences = 7;
  int32 max_chunk_sentences = 8;

  // Output granularity flags.
  bool store_sentence_vectors = 9;
  bool compute_centroids = 10;

  // Extensibility (section_source_priority, future params).
  optional google.protobuf.Struct config_json = 11;

  google.protobuf.Timestamp created_at = 12;
  google.protobuf.Timestamp updated_at = 13;

  // IDs of child VectorSets created during registration.
  repeated string vector_set_ids = 14;
}

message CreateSemanticConfigRequest {
  optional string id = 1;
  string name = 2;
  optional string config_id = 3;
  string embedding_model_id = 4;

  optional float similarity_threshold = 5;
  optional int32 percentile_threshold = 6;
  optional int32 min_chunk_sentences = 7;
  optional int32 max_chunk_sentences = 8;

  bool store_sentence_vectors = 9;
  bool compute_centroids = 10;

  optional google.protobuf.Struct config_json = 11;

  // Source CEL expression for child VectorSets.
  string source_cel = 12;
}

message CreateSemanticConfigResponse {
  SemanticConfig config = 1;
}

message GetSemanticConfigRequest {
  string id = 1;
  optional bool by_name = 2;
}

message GetSemanticConfigResponse {
  SemanticConfig config = 1;
}

message ListSemanticConfigsRequest {
  int32 page_size = 1;
  string page_token = 2;
}

message ListSemanticConfigsResponse {
  repeated SemanticConfig configs = 1;
  string next_page_token = 2;
}

message DeleteSemanticConfigRequest {
  string id = 1;
}

message DeleteSemanticConfigResponse {
  bool success = 1;
  string message = 2;
}
```

- [ ] **Step 2: Build protos to verify**

Run:
```bash
cd /work/core-services/pipestream-protos && ./gradlew :opensearch:build
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
cd /work/core-services/pipestream-protos
git add opensearch/proto/ai/pipestream/opensearch/v1/semantic_config.proto
git commit -m "proto: add SemanticConfigService with CRUD messages"
```

---

## Task 4: Proto — VectorSet message changes

**Files:**
- Modify: `/work/core-services/pipestream-protos/opensearch/proto/ai/pipestream/opensearch/v1/vector_set.proto`

- [ ] **Step 1: Add import and fields to VectorSet message and CreateVectorSetRequest**

Add import at top:

```protobuf
import "ai/pipestream/opensearch/v1/opensearch_document.proto";
```

Add to `VectorSet` message (after field 17, `content_signature`):

```protobuf
  // Semantic path: FK to SemanticConfig. Null for standard chunking VectorSets.
  optional string semantic_config_id = 18;

  // Granularity within a semantic config family.
  optional SemanticGranularity granularity = 19;
```

Add to `CreateVectorSetRequest` (after field 14, `content_signature`):

```protobuf
  optional string semantic_config_id = 15;
  optional SemanticGranularity granularity = 16;
```

- [ ] **Step 2: Make chunker_config_id optional on CreateVectorSetRequest**

Change field 3 from:

```protobuf
  // FK to ChunkerConfig (required, must exist).
  string chunker_config_id = 3;
```

to:

```protobuf
  // FK to ChunkerConfig. Required for standard VectorSets, omitted for semantic.
  optional string chunker_config_id = 3;
```

- [ ] **Step 3: Build and publish protos**

Run:
```bash
cd /work/core-services/pipestream-protos && ./gradlew build publishToMavenLocal
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
cd /work/core-services/pipestream-protos
git add opensearch/proto/ai/pipestream/opensearch/v1/vector_set.proto
git commit -m "proto: add semantic_config_id and granularity to VectorSet"
```

---

## Task 5: Proto — Publish all proto changes

- [ ] **Step 1: Full build and publish**

Run:
```bash
cd /work/core-services/pipestream-protos && ./gradlew clean build publishToMavenLocal
```
Expected: BUILD SUCCESSFUL. All downstream repos can now resolve the new proto classes.

- [ ] **Step 2: Commit any remaining changes**

```bash
cd /work/core-services/pipestream-protos
git add -A && git status
# Only commit if there are changes
git commit -m "proto: publish semantic config model protos"
```

---

## Task 6: Flyway migration — semantic_config table and vector_set alterations

**Files:**
- Create: `/work/core-services/pipestream-opensearch/opensearch-manager/src/main/resources/db/migration/V8__create_semantic_config_table.sql`

- [ ] **Step 1: Create the migration file**

```sql
-- V8: Add semantic_config table and modify vector_set for semantic path.
--
-- semantic_config stores semantic chunking parameters. On registration,
-- child VectorSets are eagerly created for each enabled granularity.
-- vector_set gains nullable semantic_config_id + granularity columns.
-- A CHECK constraint enforces: exactly one of chunker_config_id or
-- semantic_config_id must be non-null.

CREATE TABLE semantic_config (
    id                          VARCHAR(255) PRIMARY KEY,
    name                        VARCHAR(255) NOT NULL UNIQUE,
    config_id                   VARCHAR(255) NOT NULL UNIQUE,
    embedding_model_id          VARCHAR(255) NOT NULL
        REFERENCES embedding_model_config(id),
    similarity_threshold        REAL NOT NULL DEFAULT 0.5,
    percentile_threshold        INTEGER NOT NULL DEFAULT 20,
    min_chunk_sentences         INTEGER NOT NULL DEFAULT 2,
    max_chunk_sentences         INTEGER NOT NULL DEFAULT 30,
    store_sentence_vectors      BOOLEAN NOT NULL DEFAULT TRUE,
    compute_centroids           BOOLEAN NOT NULL DEFAULT TRUE,
    config_json                 JSONB,
    source_cel                  TEXT NOT NULL DEFAULT 'document.search_metadata.body',
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_semantic_config_embedding_model ON semantic_config(embedding_model_id);

-- Make chunker_config_id nullable for semantic VectorSets
ALTER TABLE vector_set ALTER COLUMN chunker_config_id DROP NOT NULL;

-- Add semantic path columns
ALTER TABLE vector_set ADD COLUMN semantic_config_id VARCHAR(255)
    REFERENCES semantic_config(id);
ALTER TABLE vector_set ADD COLUMN granularity VARCHAR(50);

-- Enforce: exactly one path (standard OR semantic)
ALTER TABLE vector_set ADD CONSTRAINT chk_vector_set_type CHECK (
    (chunker_config_id IS NOT NULL AND semantic_config_id IS NULL AND granularity IS NULL)
    OR
    (chunker_config_id IS NULL AND semantic_config_id IS NOT NULL AND granularity IS NOT NULL)
);

CREATE INDEX idx_vector_set_semantic_config ON vector_set(semantic_config_id);
CREATE INDEX idx_vector_set_granularity ON vector_set(granularity);
```

- [ ] **Step 2: Verify migration syntax by running the app tests**

Run:
```bash
cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:test --tests "*FlywayMigration*" 2>&1 | tail -10
```

If no Flyway-specific test exists, the migration will be validated when the full test suite runs (Task 9).

- [ ] **Step 3: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/resources/db/migration/V8__create_semantic_config_table.sql
git commit -m "db: add semantic_config table and vector_set semantic path columns"
```

---

## Task 7: Entity — SemanticConfigEntity

**Files:**
- Create: `/work/core-services/pipestream-opensearch/opensearch-manager/src/main/java/ai/pipestream/schemamanager/entity/SemanticConfigEntity.java`

- [ ] **Step 1: Create the entity class**

Follow the pattern from `ChunkerConfigEntity`:

```java
package ai.pipestream.schemamanager.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "semantic_config")
public class SemanticConfigEntity extends PanacheEntityBase {

    @Id
    @Column(name = "id")
    public String id;

    @Column(name = "name", nullable = false, unique = true)
    public String name;

    @Column(name = "config_id", nullable = false, unique = true)
    public String configId;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "embedding_model_id", nullable = false)
    public EmbeddingModelConfig embeddingModelConfig;

    @Column(name = "similarity_threshold", nullable = false)
    public float similarityThreshold;

    @Column(name = "percentile_threshold", nullable = false)
    public int percentileThreshold;

    @Column(name = "min_chunk_sentences", nullable = false)
    public int minChunkSentences;

    @Column(name = "max_chunk_sentences", nullable = false)
    public int maxChunkSentences;

    @Column(name = "store_sentence_vectors", nullable = false)
    public boolean storeSentenceVectors;

    @Column(name = "compute_centroids", nullable = false)
    public boolean computeCentroids;

    @Column(name = "config_json", columnDefinition = "JSONB")
    @JdbcTypeCode(SqlTypes.JSON)
    public String configJson;

    @Column(name = "source_cel", nullable = false, columnDefinition = "TEXT")
    public String sourceCel;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    public LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;

    public static Uni<SemanticConfigEntity> findByName(String name) {
        return find("name", name).firstResult();
    }

    public static Uni<SemanticConfigEntity> findByConfigId(String configId) {
        return find("configId", configId).firstResult();
    }

    public static Uni<List<SemanticConfigEntity>> listOrderedByCreatedDesc(int page, int pageSize) {
        return find("order by createdAt desc")
                .page(io.quarkus.panache.common.Page.of(page, pageSize))
                .list();
    }
}
```

- [ ] **Step 2: Compile to verify**

Run:
```bash
cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava 2>&1 | tail -5
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/entity/SemanticConfigEntity.java
git commit -m "feat: add SemanticConfigEntity JPA entity"
```

---

## Task 8: Entity — Modify VectorSetEntity for semantic path

**Files:**
- Modify: `/work/core-services/pipestream-opensearch/opensearch-manager/src/main/java/ai/pipestream/schemamanager/entity/VectorSetEntity.java`

- [ ] **Step 1: Make chunkerConfig nullable, add semanticConfig + granularity**

Change the `chunkerConfig` field annotation from:

```java
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "chunker_config_id", nullable = false)
    public ChunkerConfigEntity chunkerConfig;
```

to:

```java
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "chunker_config_id")
    public ChunkerConfigEntity chunkerConfig;
```

Add new fields after `chunkerConfig`:

```java
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "semantic_config_id")
    public SemanticConfigEntity semanticConfig;

    @Column(name = "granularity")
    public String granularity;
```

- [ ] **Step 2: Add finder method for semantic path**

Add static method:

```java
    public static Uni<VectorSetEntity> findBySemanticConfigAndGranularity(String semanticConfigId, String granularity) {
        return find("semanticConfig.id = ?1 and granularity = ?2", semanticConfigId, granularity).firstResult();
    }
```

- [ ] **Step 3: Compile to verify**

Run:
```bash
cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava 2>&1 | tail -5
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/entity/VectorSetEntity.java
git commit -m "feat: add semantic_config_id and granularity to VectorSetEntity"
```

---

## Task 9: Service — SemanticConfigServiceEngine (business logic)

**Files:**
- Create: `/work/core-services/pipestream-opensearch/opensearch-manager/src/main/java/ai/pipestream/schemamanager/SemanticConfigServiceEngine.java`

- [ ] **Step 1: Create the service engine**

```java
package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.SemanticConfigEntity;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class SemanticConfigServiceEngine {

    private static final Logger LOG = Logger.getLogger(SemanticConfigServiceEngine.class);

    private static final String[] GRANULARITIES_ALWAYS = {"SEMANTIC_CHUNK"};
    private static final String[] GRANULARITIES_SENTENCE = {"SENTENCE"};
    private static final String[] GRANULARITIES_CENTROIDS = {"PARAGRAPH", "SECTION", "DOCUMENT"};

    @WithTransaction
    public Uni<CreateSemanticConfigResponse> createSemanticConfig(CreateSemanticConfigRequest request) {
        return Panache.withTransaction(() ->
            EmbeddingModelConfig.<EmbeddingModelConfig>findById(request.getEmbeddingModelId())
                .onItem().transformToUni(emc -> {
                    if (emc == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Embedding model config not found: " + request.getEmbeddingModelId())
                                .asRuntimeException());
                    }

                    String id = request.hasId() && !request.getId().isBlank()
                            ? request.getId() : UUID.randomUUID().toString();
                    String configId = request.hasConfigId() && !request.getConfigId().isBlank()
                            ? request.getConfigId() : "semantic-" + emc.name;

                    SemanticConfigEntity entity = new SemanticConfigEntity();
                    entity.id = id;
                    entity.name = request.getName();
                    entity.configId = configId;
                    entity.embeddingModelConfig = emc;
                    entity.similarityThreshold = request.hasSimilarityThreshold()
                            ? request.getSimilarityThreshold() : 0.5f;
                    entity.percentileThreshold = request.hasPercentileThreshold()
                            ? request.getPercentileThreshold() : 20;
                    entity.minChunkSentences = request.hasMinChunkSentences()
                            ? request.getMinChunkSentences() : 2;
                    entity.maxChunkSentences = request.hasMaxChunkSentences()
                            ? request.getMaxChunkSentences() : 30;
                    entity.storeSentenceVectors = request.getStoreSentenceVectors();
                    entity.computeCentroids = request.getComputeCentroids();
                    entity.sourceCel = request.getSourceCel().isBlank()
                            ? "document.search_metadata.body" : request.getSourceCel();
                    if (request.hasConfigJson()) {
                        try {
                            entity.configJson = JsonFormat.printer().print(request.getConfigJson());
                        } catch (InvalidProtocolBufferException e) {
                            entity.configJson = null;
                        }
                    }

                    return entity.<SemanticConfigEntity>persist()
                            .chain(saved -> createChildVectorSets(saved, emc))
                            .map(vectorSetIds -> buildResponse(entity, vectorSetIds));
                })
        )
        .onFailure().transform(err -> {
            if (isConstraintViolation(err)) {
                return Status.ALREADY_EXISTS
                        .withDescription("Semantic config with that name or config_id already exists")
                        .asRuntimeException();
            }
            return err;
        });
    }

    private Uni<List<String>> createChildVectorSets(SemanticConfigEntity config, EmbeddingModelConfig emc) {
        List<String> granularities = new ArrayList<>();
        for (String g : GRANULARITIES_ALWAYS) granularities.add(g);
        if (config.storeSentenceVectors) {
            for (String g : GRANULARITIES_SENTENCE) granularities.add(g);
        }
        if (config.computeCentroids) {
            for (String g : GRANULARITIES_CENTROIDS) granularities.add(g);
        }

        List<String> createdIds = new ArrayList<>();
        Uni<Void> chain = Uni.createFrom().voidItem();

        for (String granularity : granularities) {
            chain = chain.chain(() -> {
                VectorSetEntity vs = new VectorSetEntity();
                vs.id = UUID.randomUUID().toString();
                vs.name = config.configId + "-" + granularity.toLowerCase();
                vs.semanticConfig = config;
                vs.granularity = granularity;
                vs.embeddingModelConfig = emc;
                vs.fieldName = "vs_" + config.configId + "_" + granularity.toLowerCase();
                vs.resultSetName = "default";
                vs.sourceCel = config.sourceCel;
                vs.vectorDimensions = emc.dimensions;
                vs.provenance = "SEMANTIC_CONFIG";

                createdIds.add(vs.id);
                return vs.<VectorSetEntity>persist().replaceWithVoid();
            });
        }

        return chain.replaceWith(createdIds);
    }

    @WithSession
    public Uni<GetSemanticConfigResponse> getSemanticConfig(GetSemanticConfigRequest request) {
        Uni<SemanticConfigEntity> lookup = request.hasByName() && request.getByName()
                ? SemanticConfigEntity.findByName(request.getId())
                : SemanticConfigEntity.findById(request.getId());

        return lookup.onItem().transformToUni(e -> {
            if (e == null) {
                return Uni.createFrom().failure(Status.NOT_FOUND
                        .withDescription("SemanticConfig not found: " + request.getId())
                        .asRuntimeException());
            }
            return VectorSetEntity.list("semanticConfig.id", e.id)
                    .map(vsList -> {
                        List<String> vsIds = vsList.stream()
                                .map(vs -> ((VectorSetEntity) vs).id).toList();
                        return GetSemanticConfigResponse.newBuilder()
                                .setConfig(toProto(e, vsIds))
                                .build();
                    });
        });
    }

    @WithSession
    public Uni<ListSemanticConfigsResponse> listSemanticConfigs(ListSemanticConfigsRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int page = parsePageToken(request.getPageToken());

        return SemanticConfigEntity.listOrderedByCreatedDesc(page, pageSize)
                .map(entities -> {
                    ListSemanticConfigsResponse.Builder b = ListSemanticConfigsResponse.newBuilder()
                            .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "");
                    for (SemanticConfigEntity e : entities) {
                        b.addConfigs(toProto(e, List.of()));
                    }
                    return b.build();
                });
    }

    @WithTransaction
    public Uni<DeleteSemanticConfigResponse> deleteSemanticConfig(DeleteSemanticConfigRequest request) {
        return Panache.withTransaction(() ->
                SemanticConfigEntity.<SemanticConfigEntity>findById(request.getId())
                        .onItem().transformToUni(e -> {
                            if (e == null) {
                                return Uni.createFrom().item(DeleteSemanticConfigResponse.newBuilder()
                                        .setSuccess(false).setMessage("Not found: " + request.getId()).build());
                            }
                            // Delete child VectorSets first (cascade should handle, but be explicit)
                            return VectorSetEntity.delete("semanticConfig.id", e.id)
                                    .chain(() -> e.delete())
                                    .replaceWith(DeleteSemanticConfigResponse.newBuilder()
                                            .setSuccess(true).setMessage("Deleted").build());
                        }));
    }

    private SemanticConfig toProto(SemanticConfigEntity e, List<String> vectorSetIds) {
        SemanticConfig.Builder b = SemanticConfig.newBuilder()
                .setId(e.id)
                .setName(e.name)
                .setConfigId(e.configId)
                .setEmbeddingModelId(e.embeddingModelConfig != null ? e.embeddingModelConfig.id : "")
                .setSimilarityThreshold(e.similarityThreshold)
                .setPercentileThreshold(e.percentileThreshold)
                .setMinChunkSentences(e.minChunkSentences)
                .setMaxChunkSentences(e.maxChunkSentences)
                .setStoreSentenceVectors(e.storeSentenceVectors)
                .setComputeCentroids(e.computeCentroids)
                .addAllVectorSetIds(vectorSetIds);
        if (e.createdAt != null) b.setCreatedAt(toTimestamp(e.createdAt));
        if (e.updatedAt != null) b.setUpdatedAt(toTimestamp(e.updatedAt));
        if (e.configJson != null && !e.configJson.isBlank()) {
            try {
                Struct.Builder sb = Struct.newBuilder();
                JsonFormat.parser().merge(e.configJson, sb);
                b.setConfigJson(sb.build());
            } catch (InvalidProtocolBufferException ex) {
                LOG.warnf("Could not parse config_json for SemanticConfig %s: %s", e.id, ex.getMessage());
            }
        }
        return b.build();
    }

    private com.google.protobuf.Timestamp toTimestamp(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private int parsePageToken(String token) {
        if (token == null || token.isBlank()) return 0;
        try { return Math.max(0, Integer.parseInt(token)); }
        catch (NumberFormatException e) { return 0; }
    }

    private static boolean isConstraintViolation(Throwable t) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("23505") || msg.contains("unique constraint") || msg.contains("duplicate key"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private CreateSemanticConfigResponse buildResponse(SemanticConfigEntity entity, List<String> vectorSetIds) {
        return CreateSemanticConfigResponse.newBuilder()
                .setConfig(toProto(entity, vectorSetIds))
                .build();
    }
}
```

- [ ] **Step 2: Compile to verify**

Run:
```bash
cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava 2>&1 | tail -5
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/SemanticConfigServiceEngine.java
git commit -m "feat: add SemanticConfigServiceEngine with child VectorSet creation"
```

---

## Task 10: Service — SemanticConfigServiceImpl (gRPC wrapper)

**Files:**
- Create: `/work/core-services/pipestream-opensearch/opensearch-manager/src/main/java/ai/pipestream/schemamanager/SemanticConfigServiceImpl.java`

- [ ] **Step 1: Create the gRPC service impl**

Follow the thin-delegation pattern from `VectorSetServiceImpl`:

```java
package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@GrpcService
public class SemanticConfigServiceImpl extends MutinySemanticConfigServiceGrpc.SemanticConfigServiceImplBase {

    @Inject
    SemanticConfigServiceEngine engine;

    @Override
    public Uni<CreateSemanticConfigResponse> createSemanticConfig(CreateSemanticConfigRequest request) {
        return engine.createSemanticConfig(request);
    }

    @Override
    public Uni<GetSemanticConfigResponse> getSemanticConfig(GetSemanticConfigRequest request) {
        return engine.getSemanticConfig(request);
    }

    @Override
    public Uni<ListSemanticConfigsResponse> listSemanticConfigs(ListSemanticConfigsRequest request) {
        return engine.listSemanticConfigs(request);
    }

    @Override
    public Uni<DeleteSemanticConfigResponse> deleteSemanticConfig(DeleteSemanticConfigRequest request) {
        return engine.deleteSemanticConfig(request);
    }
}
```

- [ ] **Step 2: Compile to verify**

Run:
```bash
cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava 2>&1 | tail -5
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/SemanticConfigServiceImpl.java
git commit -m "feat: add SemanticConfigServiceImpl gRPC wrapper"
```

---

## Task 11: Service — Modify VectorSetServiceEngine for nullable chunkerConfig

**Files:**
- Modify: `/work/core-services/pipestream-opensearch/opensearch-manager/src/main/java/ai/pipestream/schemamanager/VectorSetServiceEngine.java`

- [ ] **Step 1: Make chunkerConfig validation conditional in createVectorSet**

In `createVectorSet`, the current code unconditionally looks up `ChunkerConfigEntity` by `request.getChunkerConfigId()`. Change it to skip the lookup when `semantic_config_id` is set.

Find the section starting at line 48 (`return ChunkerConfigEntity.<ChunkerConfigEntity>findById(request.getChunkerConfigId())`) and wrap it in a conditional:

```java
// If this is a semantic VectorSet, skip chunker config validation
boolean isSemantic = request.hasSemanticConfigId() && !request.getSemanticConfigId().isBlank();

Uni<ChunkerConfigEntity> chunkerLookup;
if (isSemantic) {
    chunkerLookup = Uni.createFrom().item((ChunkerConfigEntity) null);
} else {
    chunkerLookup = ChunkerConfigEntity.<ChunkerConfigEntity>findById(request.getChunkerConfigId())
            .onItem().transformToUni(cc -> {
                if (cc == null) {
                    return Uni.createFrom().<ChunkerConfigEntity>failure(Status.NOT_FOUND
                            .withDescription("Chunker config not found: " + request.getChunkerConfigId())
                            .asRuntimeException());
                }
                return Uni.createFrom().item(cc);
            });
}
```

Then in the entity population, set `entity.chunkerConfig = cc` (which may be null for semantic), and also set:

```java
if (isSemantic) {
    entity.semanticConfig = /* look up SemanticConfigEntity by request.getSemanticConfigId() */;
    entity.granularity = request.hasGranularity()
            ? request.getGranularity().name().replace("SEMANTIC_GRANULARITY_", "")
            : null;
}
```

- [ ] **Step 2: Update toVectorSetProto to include semantic fields**

In the `toVectorSetProto(VectorSetEntity e, String indexName)` method, add after existing field mappings:

```java
if (e.semanticConfig != null) {
    b.setSemanticConfigId(e.semanticConfig.id);
}
if (e.granularity != null && !e.granularity.isBlank()) {
    b.setGranularity(mapGranularity(e.granularity));
}
```

Add helper method:

```java
private static SemanticGranularity mapGranularity(String g) {
    return switch (g) {
        case "SEMANTIC_CHUNK" -> SemanticGranularity.SEMANTIC_GRANULARITY_SEMANTIC_CHUNK;
        case "SENTENCE" -> SemanticGranularity.SEMANTIC_GRANULARITY_SENTENCE;
        case "PARAGRAPH" -> SemanticGranularity.SEMANTIC_GRANULARITY_PARAGRAPH;
        case "SECTION" -> SemanticGranularity.SEMANTIC_GRANULARITY_SECTION;
        case "DOCUMENT" -> SemanticGranularity.SEMANTIC_GRANULARITY_DOCUMENT;
        default -> SemanticGranularity.SEMANTIC_GRANULARITY_UNSPECIFIED;
    };
}
```

- [ ] **Step 3: Compile to verify**

Run:
```bash
cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava 2>&1 | tail -5
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/VectorSetServiceEngine.java
git commit -m "feat: VectorSetServiceEngine supports nullable chunkerConfig for semantic path"
```

---

## Task 12: Service — Modify OpenSearchIndexingService for semantic resolution

**Files:**
- Modify: `/work/core-services/pipestream-opensearch/opensearch-manager/src/main/java/ai/pipestream/schemamanager/OpenSearchIndexingService.java`

- [ ] **Step 1: Add semantic resolution path in resolveVectorSetsForDocument**

In the loop over `document.getSemanticSetsList()` (around line 427), add a check before the existing `resolveOrCreateVectorSet` call:

```java
for (SemanticVectorSet vset : document.getSemanticSetsList()) {
    chain = chain.flatMap(v -> {
        // Path 1: Explicit vector_set_id (existing)
        if (vset.hasVectorSetId() && !vset.getVectorSetId().isBlank()) {
            return VectorSetEntity.<VectorSetEntity>findById(vset.getVectorSetId())
                    // ... existing code ...
        }

        // Path 2: Semantic config — lookup by (semantic_config_id, granularity)
        if (vset.hasSemanticConfigId() && !vset.getSemanticConfigId().isBlank()
                && vset.hasGranularity()
                && vset.getGranularity() != SemanticGranularity.SEMANTIC_GRANULARITY_UNSPECIFIED) {
            String granStr = vset.getGranularity().name().replace("SEMANTIC_GRANULARITY_", "");
            return VectorSetEntity.findBySemanticConfigAndGranularity(vset.getSemanticConfigId(), granStr)
                    .onItem().transformToUni(entity -> {
                        if (entity == null) {
                            LOG.warnf("No VectorSet for semantic_config=%s granularity=%s — skipping",
                                    vset.getSemanticConfigId(), granStr);
                            return Uni.createFrom().voidItem();
                        }
                        String nested = vset.hasNestedFieldName() && !vset.getNestedFieldName().isBlank()
                                ? vset.getNestedFieldName() : entity.fieldName;
                        mappings.add(new VectorSetMapping(nested, entity.vectorDimensions));
                        return ensureIndexBinding(indexName, entity, accountId, datasourceId);
                    });
        }

        // Path 3: Standard — resolve by chunk_config_id + embedding_id (existing)
        String semanticId = String.format("%s_%s_%s", ...);
        return resolveOrCreateVectorSet(semanticId, vset)
                // ... existing code ...
    });
}
```

- [ ] **Step 2: Compile to verify**

Run:
```bash
cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:compileJava 2>&1 | tail -5
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
cd /work/core-services/pipestream-opensearch
git add opensearch-manager/src/main/java/ai/pipestream/schemamanager/OpenSearchIndexingService.java
git commit -m "feat: semantic VectorSet resolution by (semantic_config_id, granularity)"
```

---

## Task 13: Run opensearch-manager tests

- [ ] **Step 1: Run the full test suite**

Run:
```bash
cd /work/core-services/pipestream-opensearch && ./gradlew :opensearch-manager:test 2>&1 | tail -20
```
Expected: BUILD SUCCESSFUL. All existing tests pass (the standard path is unchanged).

- [ ] **Step 2: Fix any failures**

If tests fail due to the VectorSetEntity `chunkerConfig` nullable change, update test fixtures to explicitly set `chunkerConfig` on standard VectorSets.

- [ ] **Step 3: Commit fixes if any**

```bash
cd /work/core-services/pipestream-opensearch
git add -A && git commit -m "fix: update tests for nullable chunkerConfig on VectorSetEntity"
```

---

## Task 14: Semantic Manager — Accept semantic_config_id and stamp outputs

**Files:**
- Modify: `/work/modules/module-semantic-manager/src/main/java/ai/pipestream/module/semanticmanager/config/SemanticManagerOptions.java`
- Modify: `/work/modules/module-semantic-manager/src/main/java/ai/pipestream/module/semanticmanager/service/SemanticIndexingOrchestrator.java`

- [ ] **Step 1: Add semantic_config_id to SemanticManagerOptions**

Add new field to the record:

```java
@JsonProperty("semantic_config_id") String semanticConfigId,
```

Update constructors to include it. Add convenience method:

```java
public boolean hasSemanticConfigId() {
    return semanticConfigId != null && !semanticConfigId.isBlank();
}
```

- [ ] **Step 2: Thread semantic_config_id through the orchestrator**

In `SemanticIndexingOrchestrator`, the `processSemanticChunkingGroup` method produces results with hard-coded chunk_config_ids. Add `semanticConfigId` parameter and set the new proto fields.

In `assembleResult` (line 923), after building the result:

```java
if (semanticConfigId != null) {
    resultBuilder.setSemanticConfigId(semanticConfigId);
}
if (granularity != null) {
    resultBuilder.setGranularity(granularity);
}
```

Change all the hard-coded chunk_config_id strings in the semantic path:

| Location | Current | Add |
|---|---|---|
| Line 1234 `.setChunkConfigId("semantic")` | Keep | Add `setSemanticConfigId(semanticConfigId)` + `setGranularity(SEMANTIC_CHUNK)` on the result |
| Line 1255 `assembleResult(..., "sentence", ...)` | Keep | Add `setSemanticConfigId(semanticConfigId)` + `setGranularity(SENTENCE)` on the result |
| Line 1376 `.setChunkConfigId(granularity)` | Keep | Add `setSemanticConfigId(semanticConfigId)` + map granularity string to enum |

The `chunk_config_id` strings remain for backward compatibility (they're used for result set naming and display). The new `semantic_config_id` + `granularity` fields are what opensearch-manager uses for resolution.

- [ ] **Step 3: Compile to verify**

Run:
```bash
cd /work/modules/module-semantic-manager && ./gradlew compileJava 2>&1 | tail -5
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Run tests**

Run:
```bash
cd /work/modules/module-semantic-manager && ./gradlew test 2>&1 | tail -20
```
Expected: Tests pass. Existing tests don't set `semantic_config_id` so the new fields are simply unset.

- [ ] **Step 5: Commit**

```bash
cd /work/modules/module-semantic-manager
git add -A
git commit -m "feat: stamp semantic outputs with semantic_config_id and granularity"
```

---

## Task 15: OpenSearch Sink — Propagate semantic fields

**Files:**
- Modify: `/work/modules/module-opensearch-sink/src/main/java/ai/pipestream/module/opensearchsink/service/DocumentConverterService.java`

- [ ] **Step 1: Propagate semantic_config_id and granularity from result to vector set**

In `populateSemanticSets` method, where `SemanticVectorSet.Builder` is constructed (around the line `setBuilder.setEmbeddingId(embeddingId)`), add:

```java
if (result.hasSemanticConfigId() && !result.getSemanticConfigId().isEmpty()) {
    setBuilder.setSemanticConfigId(result.getSemanticConfigId());
}
if (result.hasGranularity()) {
    setBuilder.setGranularity(result.getGranularity());
}
```

- [ ] **Step 2: Compile to verify**

Run:
```bash
cd /work/modules/module-opensearch-sink && ./gradlew compileJava 2>&1 | tail -5
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Run tests**

Run:
```bash
cd /work/modules/module-opensearch-sink && ./gradlew test 2>&1 | tail -20
```
Expected: Tests pass.

- [ ] **Step 4: Commit**

```bash
cd /work/modules/module-opensearch-sink
git add -A
git commit -m "feat: propagate semantic_config_id and granularity to SemanticVectorSet"
```

---

## Task 16: Testing Sidecar — Register semantic config in E2E

**Files:**
- Modify: `/work/modules/module-testing-sidecar/src/main/java/ai/pipestream/module/pipelineprobe/e2e/E2EPipelineTestService.java`
- Modify: `/work/modules/module-testing-sidecar/src/main/java/ai/pipestream/module/pipelineprobe/e2e/SemanticChunkingDirectiveBuilder.java`

- [ ] **Step 1: Add semantic config registration in registerConfigs**

After the existing chunker/embedding config registration, when `includeSemanticChunking` is true, also register a semantic config:

```java
if (includeSemanticChunking) {
    chain = chain
            .chain(() -> createChunkerConfig("semantic", semanticChunkerJson))
            .chain(() -> createSemanticConfig(
                    "default-semantic",
                    "all-MiniLM-L6-v2",
                    "document.search_metadata.body"));
}
```

Add the `createSemanticConfig` helper method:

```java
private Uni<Void> createSemanticConfig(String name, String embeddingModelId, String sourceCel) {
    CreateSemanticConfigRequest request = CreateSemanticConfigRequest.newBuilder()
            .setName(name)
            .setConfigId(name)
            .setEmbeddingModelId(embeddingModelId)
            .setStoreSentenceVectors(true)
            .setComputeCentroids(true)
            .setSourceCel(sourceCel)
            .build();

    return semanticConfigClient.createSemanticConfig(request)
            .onItem().invoke(resp -> LOG.infof("Registered semantic config: %s with %d VectorSets",
                    resp.getConfig().getName(), resp.getConfig().getVectorSetIdsCount()))
            .onFailure().invoke(err -> LOG.warnf("Semantic config registration failed (may already exist): %s",
                    err.getMessage()))
            .onFailure().recoverWithNull()
            .replaceWithVoid();
}
```

This requires adding a `SemanticConfigService` gRPC client inject to the class.

- [ ] **Step 2: Pass semantic_config_id in the graph's semantic manager config**

In `createGraph`, when building the `semanticConfig` Struct, add the `semantic_config_id` field:

```java
if (request.getIncludeSemanticChunking()) {
    semanticConfig = SemanticChunkingDirectiveBuilder.addSemanticDirective(
            semanticConfig, "all-MiniLM-L6-v2");
    // Add semantic_config_id to the options so the orchestrator stamps outputs
    semanticConfig = semanticConfig.toBuilder()
            .putFields("semantic_config_id",
                    Value.newBuilder().setStringValue("default-semantic").build())
            .build();
}
```

- [ ] **Step 3: Compile to verify**

Run:
```bash
cd /work/modules/module-testing-sidecar && ./gradlew compileJava 2>&1 | tail -5
```
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
cd /work/modules/module-testing-sidecar
git add -A
git commit -m "feat: register semantic config in E2E and pass ID through pipeline"
```

---

## Task 17: Integration test — E2E semantic flow

- [ ] **Step 1: Clean test indices**

Run via the sidecar UI or directly:
```bash
# Delete test indices from OpenSearch
curl -X DELETE 'http://localhost:9200/test-e2e-*'
```

- [ ] **Step 2: Restart services**

Restart opensearch-manager (for Flyway migration + new gRPC service), semantic-manager, opensearch-sink, and testing-sidecar.

- [ ] **Step 3: Run standard E2E test (no semantic)**

Via the sidecar UI, run a standard E2E pipeline test without semantic chunking. Verify it passes — the standard path must be unbroken.

- [ ] **Step 4: Run semantic E2E test**

Via the sidecar UI, run E2E with "Standard + Semantic" selected. Verify:
- No "Chunker config not found" errors in opensearch-manager logs
- Opensearch-manager logs show semantic VectorSet resolution by `(semantic_config_id, granularity)`
- Document appears in OpenSearch with nested vector fields for all enabled granularities
- Sidecar reports success

- [ ] **Step 5: Commit any remaining fixes**

```bash
# Across all repos as needed
git add -A && git commit -m "fix: integration fixes for semantic config E2E flow"
```

---

## Verification Checklist

1. Standard E2E (no semantic) still passes — regression-free
2. Semantic E2E produces no "Chunker config not found" errors
3. `semantic_config` table populated with one row after registration
4. `vector_set` table has child rows with `semantic_config_id` + `granularity` set, `chunker_config_id` null
5. `vector_set_index_binding` rows created for semantic VectorSets on first document
6. OpenSearch index has nested KNN mappings for each semantic granularity
7. opensearch-manager unit tests pass
8. semantic-manager unit tests pass
9. opensearch-sink unit tests pass

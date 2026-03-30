# pipestream-opensearch

OpenSearch indexing and search management for the Pipestream platform. This repo contains two components as a Gradle composite build:

- **opensearch-manager** -- Quarkus service that owns OpenSearch indices, vector set configuration, and document indexing
- **pipestream-opensearch-client** -- Quarkus extension that provides a gRPC client for talking to OpenSearch's native protobuf API

## Repository Structure

```
pipestream-opensearch/
  opensearch-manager/          # Quarkus service (gRPC + Kafka + PostgreSQL)
    src/main/java/             # Service implementation
    src/main/resources/db/     # Flyway migrations (V1-V7)
    src/main/docker/           # Dockerfile.jvm
    src/test/java/             # Unit + QuarkusTest tests
  pipestream-opensearch-client/
    runtime/                   # Quarkus extension runtime module
    deployment/                # Quarkus extension deployment module
  build.gradle                 # Composite build aggregator
  settings.gradle              # includeBuild declarations
```

## opensearch-manager

The manager is the single point of contact for all OpenSearch operations in the platform. Pipeline modules (like the opensearch-sink) send documents here via gRPC; the manager handles schema management, vector set resolution, and the actual indexing.

### gRPC Services

**OpenSearchManagerService** -- Index and document lifecycle:
- `IndexDocument` / `StreamIndexDocuments` -- index `OpenSearchDocument` protos with automatic vector set resolution
- `IndexAnyDocument` -- index arbitrary protobuf messages as JSON
- `CreateIndex` / `DeleteIndex` / `IndexExists` / `ListIndices`
- `GetIndexStats` / `GetIndexMapping`
- `GetOpenSearchDocument` / `DeleteDocument`
- `SearchFilesystemMeta` / `SearchDocumentUploads`

**VectorSetService** -- Vector set configuration (CRUD + resolution):
- `CreateVectorSet` / `GetVectorSet` / `UpdateVectorSet` / `DeleteVectorSet` / `ListVectorSets`
- `ResolveVectorSet` / `ResolveVectorSetFromDirective`

**ChunkerConfigService** -- Chunker configuration registry (CRUD)

**EmbeddingConfigService** -- Embedding model configs and index bindings (CRUD)

### How Indexing Works

1. The opensearch-sink converts a `PipeDoc` into an `OpenSearchDocument` proto (separate schema, designed for search)
2. The sink sends the `OpenSearchDocument` to the manager via `IndexDocument`
3. The manager resolves vector sets -- matching each `(source_field, chunker, embedder)` triple to a registered `VectorSet` with known dimensions
4. Creates KNN-enabled nested field mappings in OpenSearch if they don't exist
5. Transforms the document JSON: moves embeddings from `semantic_sets[]` into named nested fields (`vs_body_token500_miniLM`)
6. Strips fields that would cause OpenSearch mapping conflicts (e.g., `punctuation_counts` with dot-character keys)
7. Indexes via OpenSearch's native gRPC protobuf API

### Database

PostgreSQL with Flyway migrations. Tables:

| Table | Purpose |
|-------|---------|
| `embedding_model_config` | Registered embedding models (name, dimensions, provider) |
| `chunker_config` | Registered chunker configurations |
| `vector_set` | Named vector sets binding a chunker + embedder pair |
| `vector_set_index_binding` | Which vector sets are bound to which indices |
| `index_embedding_binding` | Embedding model -> index mappings |

### Kafka Consumers

The manager listens on several Kafka topics for real-time event processing:

| Topic | Handler | Purpose |
|-------|---------|---------|
| `drive-updates` | `RepositoryUpdateConsumer` | Index filesystem drive/node metadata |
| `repository-events` | `RepositoryUpdateConsumer` | Index document upload events |
| `pipedoc-updates` | `RepositoryUpdateConsumer` | Index PipeDoc metadata to catalog |
| `module-updates` | `PipelineEventConsumer` | Track module registration changes |
| `graph-updates` | `PipelineEventConsumer` | Track pipeline graph changes |
| `process-request-updates` | `PipelineEventConsumer` | Index processing history |
| `process-response-updates` | `PipelineEventConsumer` | Index processing history |
| `document-uploaded-events` | `PipelineEventConsumer` | Index upload events |

All Kafka messages are protobuf-encoded with deterministic UUID keys via Apicurio Registry.

### Analytics Fields

`OpenSearchDocument` includes three levels of NLP analytics computed upstream by the chunker:

- **`source_field_analytics[]`** -- document-level stats per source field (word count, language, POS densities, vocabulary density)
- **`nlp_analysis`** -- sentence spans, language detection, POS distribution from the NLP pass
- **`chunk_analytics`** on each nested embedding -- per-chunk positional features, text stats, POS ratios

These are indexed as searchable/filterable fields in OpenSearch. `punctuation_counts` maps are stripped before indexing because their keys (`.`, `,`, `!`) conflict with OpenSearch's dot-path field name interpretation.

## pipestream-opensearch-client

A Quarkus extension that wraps OpenSearch's experimental native gRPC protobuf API. Provides:

- Managed gRPC channel to OpenSearch (via `quarkus-dynamic-grpc` service discovery)
- DevServices integration -- automatically starts an OpenSearch container in dev/test mode
- Proto stubs generated from [opensearch-protobufs](https://github.com/opensearch-project/opensearch-protobufs)

This extension lives in this repo as a composite build and is consumed by the opensearch-manager via `includeBuild`. It is not published to the platform BOM -- only the opensearch-manager uses it.

## Prerequisites

- Java 21+
- Docker (for DevServices: PostgreSQL, OpenSearch, Kafka, Apicurio Registry)
- Access to `ai-pipestream` GitHub packages (for `pipestream-bom`)

## Build

```bash
# Build everything
./gradlew build

# Build only opensearch-manager
cd opensearch-manager && ./gradlew build

# Run tests
./gradlew test

# Publish to Maven Local (for local development)
./gradlew publishToMavenLocal
```

Proto stubs are generated at build time from [pipestream-protos](https://github.com/ai-pipestream/pipestream-protos) via the `proto-toolchain` plugin. After proto changes:

```bash
cd opensearch-manager && ./gradlew clean fetchProtos generateProtos build
```

## Run

```bash
cd opensearch-manager && quarkus dev
```

DevServices will start PostgreSQL, OpenSearch, Kafka, and Apicurio Registry automatically. The service runs on port **18103** by default.

## Docker

```bash
cd opensearch-manager && ../gradlew build -x test
docker build -f opensearch-manager/src/main/docker/Dockerfile.jvm -t opensearch-manager .
```

CI builds multi-arch images (amd64 + arm64) and pushes to `ghcr.io/ai-pipestream/opensearch-manager`.

## Configuration

Key environment variables for production:

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENSEARCH_MANAGER_DB_URL` | `jdbc:postgresql://localhost:5432/opensearch_manager` | PostgreSQL JDBC URL |
| `OPENSEARCH_MANAGER_DB_USER` | `pipeline` | Database username |
| `OPENSEARCH_MANAGER_DB_PASSWORD` | `password` | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka brokers |
| `OPENSEARCH_GRPC_ADDRESS` | (via service discovery) | OpenSearch gRPC endpoint |

## License

MIT

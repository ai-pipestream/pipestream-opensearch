# opensearch-manager gRPC Test Scripts

These scripts use [grpcurl](https://github.com/fullstorydev/grpcurl) to test the opensearch-manager gRPC API. Install grpcurl first:

```bash
# macOS
brew install grpcurl

# or download from https://github.com/fullstorydev/grpcurl/releases
```

## Default host

- **localhost:18103** (from `%dev.quarkus.http.port`)

Override by passing host:port as the first argument where supported.

## Scripts

### Health Check
| Script | Description |
|--------|-------------|
| `verify-service-health.sh` | Comprehensive health check: gRPC availability, DB persistence (EmbeddingConfig + Bindings), OpenSearch connectivity |

### Read/List
| Script | Description |
|--------|-------------|
| `grpcurl-list-services.sh` | List all gRPC services |
| `grpcurl-list-indices.sh` | List OpenSearch indices (optional prefix filter) |
| `grpcurl-index-exists.sh` | Check if an index exists |
| `grpcurl-index-stats.sh` | Get document count and stats for an index |
| `grpcurl-embedding-configs.sh` | List embedding model configs |
| `grpcurl-embedding-bindings.sh` | List index-embedding bindings |
| `grpcurl-chunker-configs.sh` | List chunker configs |

### Create
| Script | Description |
|--------|-------------|
| `grpcurl-create-index.sh` | Create index with vector field (dimension) |
| `grpcurl-create-embedding-config.sh` | Create embedding model config |
| `grpcurl-create-embedding-binding.sh` | Create index-embedding binding |
| `grpcurl-create-chunker-config.sh` | Create chunker config |
| `grpcurl-delete-index.sh` | Delete an index (if implemented) |

### ChunkerConfig
| Script | Description |
|--------|-------------|
| `grpcurl-get-chunker-config.sh` | Get chunker config by ID or name |
| `grpcurl-delete-chunker-config.sh` | Delete chunker config by ID |

### Test
| Script | Description |
|--------|-------------|
| `test-create-and-verify.sh` | Full e2e: create config, index, binding, verify, cleanup. Note: ListIndices, GetIndexStats, DeleteIndex may be unimplemented. |

## Examples

```bash
# List services
./grpcurl-list-services.sh

# List indices (all or filtered)
./grpcurl-list-indices.sh
./grpcurl-list-indices.sh localhost:18103 pipeline-

# Check if index exists
./grpcurl-index-exists.sh repository-pipedocs

# Get index stats
./grpcurl-index-stats.sh repository-pipedocs

# List embedding configs and bindings
./grpcurl-embedding-configs.sh
./grpcurl-embedding-bindings.sh localhost:18103 repository-pipedocs

# Chunker configs
./grpcurl-chunker-configs.sh
./grpcurl-create-chunker-config.sh "token-512-50" "token-body-512-50"
./grpcurl-get-chunker-config.sh "token-body-512-50"
./grpcurl-get-chunker-config.sh "token-512-50" --by-name
```

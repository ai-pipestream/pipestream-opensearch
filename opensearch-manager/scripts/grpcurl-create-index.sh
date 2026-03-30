#!/usr/bin/env bash
# Create an OpenSearch index with vector search (embeddings field).
# Usage: ./grpcurl-create-index.sh <index_name> [dimension] [host:port]
# Example: ./grpcurl-create-index.sh test-index-v1 384
# dimension defaults to 384

set -e
INDEX_NAME="${1:?Usage: $0 <index_name> [dimension] [host:port]}"
DIMENSION="${2:-384}"
HOST="${3:-localhost:18103}"

grpcurl -plaintext "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/CreateIndex "{
  \"index_name\": \"$INDEX_NAME\",
  \"vector_field_definition\": {
    \"dimension\": $DIMENSION
  }
}"

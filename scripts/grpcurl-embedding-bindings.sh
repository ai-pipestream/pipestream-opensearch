#!/usr/bin/env bash
# List index-embedding bindings (optionally filtered by index name).
# Usage: ./grpcurl-embedding-bindings.sh [host:port] [index_name] [page_size]
# Example: ./grpcurl-embedding-bindings.sh localhost:18103 repository-pipedocs

set -e
HOST="${1:-localhost:18103}"
INDEX_NAME="${2:-}"
PAGE_SIZE="${3:-10}"
if [[ -n "$INDEX_NAME" ]]; then
  grpcurl -plaintext -d "{\"index_name\": \"$INDEX_NAME\", \"page_size\": $PAGE_SIZE}" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/ListIndexEmbeddingBindings
else
  grpcurl -plaintext -d "{\"page_size\": $PAGE_SIZE}" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/ListIndexEmbeddingBindings
fi

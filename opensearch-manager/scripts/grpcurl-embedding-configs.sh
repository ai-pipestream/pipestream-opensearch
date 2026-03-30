#!/usr/bin/env bash
# List embedding model configs.
# Usage: ./grpcurl-embedding-configs.sh [host:port] [page_size]
# Example: ./grpcurl-embedding-configs.sh localhost:18103 20

set -e
HOST="${1:-localhost:18103}"
PAGE_SIZE="${2:-10}"
grpcurl -plaintext -d "{\"page_size\": $PAGE_SIZE}" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/ListEmbeddingModelConfigs

#!/usr/bin/env bash
# List OpenSearch indices (optionally filtered by prefix).
# Usage: ./grpcurl-list-indices.sh [host:port] [prefix_filter]
# Example: ./grpcurl-list-indices.sh localhost:18103 pipeline-

set -e
HOST="${1:-localhost:18103}"
PREFIX="${2:-}"
if [[ -n "$PREFIX" ]]; then
  grpcurl -plaintext "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/ListIndices "{\"prefix_filter\": \"$PREFIX\"}"
else
  grpcurl -plaintext "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/ListIndices '{}'
fi

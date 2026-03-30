#!/usr/bin/env bash
# Get stats for an OpenSearch index.
# Usage: ./grpcurl-index-stats.sh <index_name> [host:port]
# Example: ./grpcurl-index-stats.sh repository-pipedocs

set -e
INDEX_NAME="${1:?Usage: $0 <index_name> [host:port]}"
HOST="${2:-localhost:18103}"
grpcurl -plaintext "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/GetIndexStats "{\"index_name\": \"$INDEX_NAME\"}"

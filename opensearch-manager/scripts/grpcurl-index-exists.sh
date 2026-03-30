#!/usr/bin/env bash
# Check if an OpenSearch index exists.
# Usage: ./grpcurl-index-exists.sh <index_name> [host:port]
# Example: ./grpcurl-index-exists.sh repository-pipedocs

set -e
INDEX_NAME="${1:?Usage: $0 <index_name> [host:port]}"
HOST="${2:-localhost:18103}"
grpcurl -plaintext "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/IndexExists "{\"index_name\": \"$INDEX_NAME\"}"

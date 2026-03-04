#!/usr/bin/env bash
# Delete an OpenSearch index.
# Usage: ./grpcurl-delete-index.sh <index_name> [host:port]
# Example: ./grpcurl-delete-index.sh test-index-v1

set -e
INDEX_NAME="${1:?Usage: $0 <index_name> [host:port]}"
HOST="${2:-localhost:18103}"

grpcurl -plaintext "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/DeleteIndex "{
  \"index_name\": \"$INDEX_NAME\"
}"

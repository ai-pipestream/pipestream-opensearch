#!/usr/bin/env bash
# Delete a chunker config by ID.
# Usage: ./grpcurl-delete-chunker-config.sh <id> [host:port]
# Example: ./grpcurl-delete-chunker-config.sh "550e8400-e29b-41d4-a716-446655440000"

set -e
ID="${1:?Usage: $0 <id> [host:port]}"
HOST="${2:-localhost:18103}"

grpcurl -plaintext -d "{\"id\": \"$ID\"}" "$HOST" ai.pipestream.opensearch.v1.ChunkerConfigService/DeleteChunkerConfig

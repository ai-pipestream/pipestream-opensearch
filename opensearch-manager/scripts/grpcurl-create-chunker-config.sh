#!/usr/bin/env bash
# Create a chunker config.
# Usage: ./grpcurl-create-chunker-config.sh <name> <config_id> [host:port]
# Example: ./grpcurl-create-chunker-config.sh "token-512-50" "token-body-512-50"
# Config JSON defaults: algorithm=token, sourceField=body, chunkSize=512, chunkOverlap=50

set -e
NAME="${1:?Usage: $0 <name> <config_id> [host:port]}"
CONFIG_ID="${2:?Usage: $0 <name> <config_id> [host:port]}"
HOST="${3:-localhost:18103}"

grpcurl -plaintext -d "{
  \"name\": \"$NAME\",
  \"config_id\": \"$CONFIG_ID\",
  \"config_json\": {
    \"fields\": {
      \"algorithm\": {\"string_value\": \"token\"},
      \"sourceField\": {\"string_value\": \"body\"},
      \"chunkSize\": {\"number_value\": 512},
      \"chunkOverlap\": {\"number_value\": 50}
    }
  }
}" "$HOST" ai.pipestream.opensearch.v1.ChunkerConfigService/CreateChunkerConfig

#!/usr/bin/env bash
# Get a chunker config by ID or name.
# Usage: ./grpcurl-get-chunker-config.sh <id_or_name> [--by-name] [host:port]
# Example: ./grpcurl-get-chunker-config.sh "token-body-512-50"
# Example: ./grpcurl-get-chunker-config.sh "my-chunker" --by-name

set -e
ID_OR_NAME="${1:?Usage: $0 <id_or_name> [--by-name] [host:port]}"
BY_NAME="false"
HOST="localhost:18103"

shift || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --by-name)
      BY_NAME="true"
      shift
      ;;
    *)
      HOST="$1"
      shift
      ;;
  esac
done

grpcurl -plaintext -d "{
  \"id\": \"$ID_OR_NAME\",
  \"by_name\": $BY_NAME
}" "$HOST" ai.pipestream.opensearch.v1.ChunkerConfigService/GetChunkerConfig

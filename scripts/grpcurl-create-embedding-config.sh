#!/usr/bin/env bash
# Create an embedding model config.
# Usage: ./grpcurl-create-embedding-config.sh <name> <model_identifier> [dimensions] [host:port]
# Example: ./grpcurl-create-embedding-config.sh "minilm-v2" "sentence-transformers/all-MiniLM-L6-v2" 384

set -e
NAME="${1:?Usage: $0 <name> <model_identifier> [dimensions] [host:port]}"
MODEL_ID="${2:?Usage: $0 <name> <model_identifier> [dimensions] [host:port]}"
DIMENSIONS="${3:-384}"
HOST="${4:-localhost:18103}"

grpcurl -plaintext -d "{
  \"name\": \"$NAME\",
  \"model_identifier\": \"$MODEL_ID\",
  \"dimensions\": $DIMENSIONS
}" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/CreateEmbeddingModelConfig

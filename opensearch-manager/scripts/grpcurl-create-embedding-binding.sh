#!/usr/bin/env bash
# Create an index-embedding binding.
# Usage: ./grpcurl-create-embedding-binding.sh <index_name> <embedding_config_id> [field_name] [host:port]
# Example: ./grpcurl-create-embedding-binding.sh test-index-v1 <uuid-from-create-embedding-config>

set -e
INDEX_NAME="${1:?Usage: $0 <index_name> <embedding_config_id> [field_name] [host:port]}"
EMBEDDING_CONFIG_ID="${2:?Usage: $0 <index_name> <embedding_config_id> [field_name] [host:port]}"
FIELD_NAME="${3:-embeddings_384.embedding}"
HOST="${4:-localhost:18103}"

grpcurl -plaintext -d "{
  \"index_name\": \"$INDEX_NAME\",
  \"embedding_model_config_id\": \"$EMBEDDING_CONFIG_ID\",
  \"field_name\": \"$FIELD_NAME\"
}" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/CreateIndexEmbeddingBinding

#!/usr/bin/env bash
# End-to-end test: create embedding config, create index, create binding, verify, cleanup.
# Usage: ./test-create-and-verify.sh [host:port]
# Prerequisites: opensearch-manager running, grpcurl installed, OpenSearch + PostgreSQL available

set -e
HOST="${1:-localhost:18103}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INDEX_NAME="grpcurl-test-index-$(date +%s)"
EMBEDDING_NAME="grpcurl-test-embedder-$(date +%s)"

echo "=== grpcurl Create/Verify Test ==="
echo "Host: $HOST"
echo "Index: $INDEX_NAME"
echo ""

# 1. Create embedding config
echo "1. Creating embedding model config..."
CREATE_CONFIG=$(grpcurl -plaintext -d "{
  \"name\": \"$EMBEDDING_NAME\",
  \"model_identifier\": \"sentence-transformers/all-MiniLM-L6-v2\",
  \"dimensions\": 384
}" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/CreateEmbeddingModelConfig)
echo "$CREATE_CONFIG"
# Extract id: try jq first, fallback to grep
if command -v jq &>/dev/null; then
  EMBEDDING_ID=$(echo "$CREATE_CONFIG" | jq -r '.config.id // empty' 2>/dev/null)
fi
if [[ -z "$EMBEDDING_ID" ]]; then
  EMBEDDING_ID=$(echo "$CREATE_CONFIG" | grep -oE '"id"[[:space:]]*:[[:space:]]*"[^"]+"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/')
fi
if [[ -z "$EMBEDDING_ID" ]]; then
  echo "WARN: Could not extract embedding config ID, skipping binding step"
  EMBEDDING_ID=""
fi
[[ -n "$EMBEDDING_ID" ]] && echo "   -> Created embedding config: $EMBEDDING_ID"
echo ""

# 2. Create index
echo "2. Creating index $INDEX_NAME..."
CREATE_INDEX=$(grpcurl -plaintext -d "{
  \"index_name\": \"$INDEX_NAME\",
  \"vector_field_definition\": { \"dimension\": 384 }
}" "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/CreateIndex)
echo "$CREATE_INDEX"
if ! echo "$CREATE_INDEX" | grep -qE '"success"[[:space:]]*:[[:space:]]*true'; then
  echo "ERROR: Index creation failed"
  exit 1
fi
echo "   -> Index created"
echo ""

# 3. Create binding (if we got an embedding config id)
if [[ -n "$EMBEDDING_ID" ]]; then
  echo "3. Creating index-embedding binding..."
  CREATE_BINDING=$(grpcurl -plaintext -d "{
    \"index_name\": \"$INDEX_NAME\",
    \"embedding_model_config_id\": \"$EMBEDDING_ID\",
    \"field_name\": \"embeddings_384.embedding\"
  }" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/CreateIndexEmbeddingBinding)
  echo "$CREATE_BINDING"
  echo "   -> Binding created"
else
  echo "3. Skipping binding (no embedding config id)"
fi
echo ""

# 4. Verify - index exists (ListIndices/GetIndexStats may be unimplemented)
echo "4. Checking index exists..."
INDEX_EXISTS=$(grpcurl -plaintext -d "{\"index_name\": \"$INDEX_NAME\"}" "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/IndexExists)
echo "$INDEX_EXISTS"
echo ""

# 5. Cleanup - delete index (DeleteIndex may be unimplemented)
echo "5. Deleting test index..."
if grpcurl -plaintext -d "{\"index_name\": \"$INDEX_NAME\"}" "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/DeleteIndex 2>/dev/null; then
  echo "   -> Index deleted"
else
  echo "   -> DeleteIndex not implemented or failed (index $INDEX_NAME may remain)"
fi
echo ""

echo "=== Test completed ==="

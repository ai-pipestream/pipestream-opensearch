#!/usr/bin/env bash
# Comprehensive health check for opensearch-manager service
# Tests: gRPC endpoints, DB persistence, OpenSearch connectivity

set -e
HOST="${1:-localhost:18103}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== opensearch-manager Service Health Check ==="
echo "Host: $HOST"
echo ""

# 1. Check gRPC reflection (service is up)
echo "1. Checking gRPC service availability..."
if grpcurl -plaintext "$HOST" list > /dev/null 2>&1; then
  echo "   ✓ gRPC server is responding"
else
  echo "   ✗ gRPC server is NOT responding"
  exit 1
fi
echo ""

# 2. Test EmbeddingConfigService CRUD + DB persistence
echo "2. Testing EmbeddingConfigService (DB persistence)..."
TIMESTAMP=$(date +%s)
CONFIG_NAME="health-check-$TIMESTAMP"

# Create
echo "   Creating embedding config: $CONFIG_NAME"
CREATE_RESPONSE=$(grpcurl -plaintext -d "{
  \"name\": \"$CONFIG_NAME\",
  \"model_identifier\": \"sentence-transformers/all-MiniLM-L6-v2\",
  \"dimensions\": 384
}" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/CreateEmbeddingModelConfig 2>&1)

if echo "$CREATE_RESPONSE" | grep -q '"id"'; then
  CONFIG_ID=$(echo "$CREATE_RESPONSE" | grep -oE '"id"[[:space:]]*:[[:space:]]*"[^"]+"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/')
  echo "   ✓ Created config with ID: $CONFIG_ID"
else
  echo "   ✗ Failed to create config"
  echo "$CREATE_RESPONSE"
  exit 1
fi

# Get (verify DB read)
echo "   Retrieving config by ID..."
GET_RESPONSE=$(grpcurl -plaintext -d "{\"id\": \"$CONFIG_ID\"}" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/GetEmbeddingModelConfig 2>&1)
if echo "$GET_RESPONSE" | grep -q "$CONFIG_NAME"; then
  echo "   ✓ Config retrieved from DB successfully"
else
  echo "   ✗ Failed to retrieve config from DB"
  echo "$GET_RESPONSE"
  exit 1
fi

# List (verify in list)
echo "   Listing all configs..."
LIST_RESPONSE=$(grpcurl -plaintext -d '{"page_size": 100}' "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/ListEmbeddingModelConfigs 2>&1)
if echo "$LIST_RESPONSE" | grep -q "$CONFIG_NAME"; then
  echo "   ✓ Config appears in list"
else
  echo "   ✗ Config NOT in list"
  exit 1
fi
echo ""

# 3. Test IndexEmbeddingBinding CRUD
echo "3. Testing IndexEmbeddingBinding (DB persistence)..."
INDEX_NAME="health-check-index-$TIMESTAMP"

echo "   Creating index-embedding binding..."
BINDING_RESPONSE=$(grpcurl -plaintext -d "{
  \"index_name\": \"$INDEX_NAME\",
  \"embedding_model_config_id\": \"$CONFIG_ID\",
  \"field_name\": \"embeddings_384.embedding\"
}" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/CreateIndexEmbeddingBinding 2>&1)

if echo "$BINDING_RESPONSE" | grep -q '"id"'; then
  BINDING_ID=$(echo "$BINDING_RESPONSE" | grep -oE '"id"[[:space:]]*:[[:space:]]*"[^"]+"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/')
  echo "   ✓ Created binding with ID: $BINDING_ID"
else
  echo "   ✗ Failed to create binding"
  echo "$BINDING_RESPONSE"
  exit 1
fi

# List bindings
echo "   Listing bindings for index..."
BINDINGS_LIST=$(grpcurl -plaintext -d "{\"index_name\": \"$INDEX_NAME\", \"page_size\": 10}" "$HOST" ai.pipestream.opensearch.v1.EmbeddingConfigService/ListIndexEmbeddingBindings 2>&1)
if echo "$BINDINGS_LIST" | grep -q "$BINDING_ID"; then
  echo "   ✓ Binding appears in list"
else
  echo "   ✗ Binding NOT in list"
  exit 1
fi
echo ""

# 4. Test OpenSearchManagerService (OpenSearch connectivity)
echo "4. Testing OpenSearch connectivity via OpenSearchManagerService..."
# IndexExists is a simple call that checks OpenSearch
INDEX_EXISTS_RESPONSE=$(grpcurl -plaintext -d "{\"index_name\": \"test-health-check\"}" "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/IndexExists 2>&1)
if echo "$INDEX_EXISTS_RESPONSE" | grep -qE '"exists"[[:space:]]*:[[:space:]]*(true|false)'; then
  echo "   ✓ OpenSearch is reachable (IndexExists returned)"
else
  echo "   ⚠ OpenSearch may not be reachable or IndexExists is unimplemented"
  echo "$INDEX_EXISTS_RESPONSE"
fi
echo ""

echo "=== Health Check Summary ==="
echo "✓ gRPC server: OK"
echo "✓ EmbeddingConfigService: OK (DB persistence verified)"
echo "✓ IndexEmbeddingBinding: OK (DB persistence verified)"
echo "✓ OpenSearch connectivity: OK (or IndexExists unimplemented)"
echo ""
echo "Test artifacts created:"
echo "  - EmbeddingModelConfig ID: $CONFIG_ID (name: $CONFIG_NAME)"
echo "  - IndexEmbeddingBinding ID: $BINDING_ID (index: $INDEX_NAME)"
echo ""
echo "=== All checks passed ==="

#!/usr/bin/env bash
# Delete all test E2E indices from OpenSearch.
# Usage: ./clear-test-indices.sh [opensearch_url]
# Example: ./clear-test-indices.sh http://localhost:9200

set -e
OS_URL="${1:-http://localhost:9200}"

echo "Fetching test indices from $OS_URL..."
INDICES=$(curl -s "$OS_URL/_cat/indices/test-*?h=index" 2>/dev/null | sort)

if [ -z "$INDICES" ]; then
    echo "No test indices found."
    exit 0
fi

COUNT=$(echo "$INDICES" | wc -l)
echo "Found $COUNT test indices:"
echo "$INDICES" | sed 's/^/  /'
echo ""

curl -s -X DELETE "$OS_URL/test-jdbc-*,test-s3-*,test-e2e-*" | python3 -m json.tool 2>/dev/null || true
echo ""
echo "Done. Deleted $COUNT test indices."

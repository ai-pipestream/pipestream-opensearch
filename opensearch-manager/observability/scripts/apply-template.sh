#!/usr/bin/env bash
# Apply the pipeline-events index template to the OpenSearch cluster.
# Re-run any time you switch clusters; it's idempotent.
#
# Usage:
#   OS_URL=http://localhost:9200 ./apply-template.sh
#   OS_URL=https://os.prod:9200 OS_USER=admin OS_PASS=... OS_INSECURE=1 ./apply-template.sh

set -euo pipefail

OS_URL="${OS_URL:-http://localhost:9200}"
OS_USER="${OS_USER:-}"
OS_PASS="${OS_PASS:-}"
OS_INSECURE="${OS_INSECURE:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_FILE="${SCRIPT_DIR}/../templates/pipeline-events.json"

if [[ ! -f "$TEMPLATE_FILE" ]]; then
  echo "ERROR: template not found at $TEMPLATE_FILE" >&2
  exit 2
fi

CURL_OPTS=(--fail-with-body -sS -X PUT "${OS_URL}/_index_template/pipeline-events"
           -H 'Content-Type: application/json'
           --data-binary "@${TEMPLATE_FILE}")

if [[ -n "$OS_USER" ]]; then
  CURL_OPTS+=(-u "${OS_USER}:${OS_PASS}")
fi
if [[ "$OS_INSECURE" == "1" ]]; then
  CURL_OPTS+=(-k)
fi

echo "==> PUT ${OS_URL}/_index_template/pipeline-events"
RESP=$(curl "${CURL_OPTS[@]}")
echo "$RESP"
echo
echo "OK. New monthly indices (pipeline-events-YYYY.MM) will pick up this template."
echo "Existing indices keep their current dynamic mappings; reindex if you want them retro-fitted."

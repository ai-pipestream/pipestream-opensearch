#!/usr/bin/env bash
# Import the pipestream pipeline-events dashboard into OpenSearch Dashboards.
# Re-run any time you switch clusters.
#
# Usage:
#   OSD_URL=http://localhost:5601 ./import-dashboards.sh
#   OSD_URL=https://dashboards.prod OSD_USER=admin OSD_PASS=... OSD_INSECURE=1 ./import-dashboards.sh
#
# The --overwrite=true flag means re-imports replace previous versions in place.

set -euo pipefail

OSD_URL="${OSD_URL:-http://localhost:5601}"
OSD_USER="${OSD_USER:-}"
OSD_PASS="${OSD_PASS:-}"
OSD_INSECURE="${OSD_INSECURE:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NDJSON_FILE="${SCRIPT_DIR}/../dashboards/pipestream-pipeline-events.ndjson"

if [[ ! -f "$NDJSON_FILE" ]]; then
  echo "ERROR: dashboard file not found at $NDJSON_FILE" >&2
  exit 2
fi

CURL_OPTS=(--fail-with-body -sS -X POST
           "${OSD_URL}/api/saved_objects/_import?overwrite=true"
           -H 'osd-xsrf: true'
           --form "file=@${NDJSON_FILE}")

if [[ -n "$OSD_USER" ]]; then
  CURL_OPTS+=(-u "${OSD_USER}:${OSD_PASS}")
fi
if [[ "$OSD_INSECURE" == "1" ]]; then
  CURL_OPTS+=(-k)
fi

echo "==> POST ${OSD_URL}/api/saved_objects/_import (file=$(basename "$NDJSON_FILE"))"
RESP=$(curl "${CURL_OPTS[@]}")
echo "$RESP"
echo
echo "OK. Open ${OSD_URL}/app/dashboards and look for 'Pipestream / Pipeline Events'."

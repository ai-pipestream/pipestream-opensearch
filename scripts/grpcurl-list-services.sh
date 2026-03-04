#!/usr/bin/env bash
# List all gRPC services exposed by opensearch-manager.
# Usage: ./grpcurl-list-services.sh [host:port]
# Default: localhost:18103

set -e
HOST="${1:-localhost:18103}"
grpcurl -plaintext "$HOST" list

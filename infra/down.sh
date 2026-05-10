#!/usr/bin/env bash
# Stop and remove all containers in the cluster.
# Usage: bash infra/down.sh
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Stopping cluster..."
docker compose -f "$DIR/docker-compose.yml" down

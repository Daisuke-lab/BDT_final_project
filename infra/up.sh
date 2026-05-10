#!/usr/bin/env bash
# Spawn the full docker-compose cluster.
# Usage: ./up.sh [extra docker compose flags]
#   e.g. ./up.sh --scale ingestion=2
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Starting cluster..."
docker compose -f "$DIR/docker-compose.yml" up -d "$@"

echo ""
echo "==> Cluster is up:"
docker compose -f "$DIR/docker-compose.yml" ps

echo ""
echo "Useful endpoints:"
echo "  Kafka (host):        localhost:29092"
echo "  HBase Master UI:     http://localhost:16010"
echo "  Visualization API:   http://localhost:8080"
echo "  Visualization UI:    http://localhost:3000"
echo ""
echo "To stop: docker compose -f infra/docker-compose.yml down"

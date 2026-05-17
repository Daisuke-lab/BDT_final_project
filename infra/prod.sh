#!/usr/bin/env bash
# Start the prod cluster (HBase + streaming + backend + frontend).
# Streaming connects to production Kafka (MSK) — no local Kafka/Zookeeper/ingestion.
# Reads credentials from infra/local.env (gitignored).
# Usage: ./prod.sh [extra docker compose flags]
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

ENV_FILE="$DIR/local.env"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. Copy infra/local.env.example and fill in your credentials." >&2
  exit 1
fi

set -o allexport
source "$ENV_FILE"
set +o allexport

echo "==> Starting prod cluster (Kafka: $KAFKA_BOOTSTRAP_SERVERS)..."
docker compose -f "$DIR/docker-compose.prod.yml" up -d "$@"

echo ""
echo "==> Cluster is up:"
docker compose -f "$DIR/docker-compose.prod.yml" ps

echo ""
echo "Useful endpoints:"
echo "  Kafka:             $KAFKA_BOOTSTRAP_SERVERS (MSK)"
echo "  HBase Master UI:   http://localhost:16010"
echo "  Visualization API: http://localhost:8080"
echo "  Visualization UI:  http://localhost:3000"
echo ""
echo "To stop: docker compose -f infra/docker-compose.prod.yml down"

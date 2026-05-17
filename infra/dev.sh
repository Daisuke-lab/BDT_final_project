#!/usr/bin/env bash
# Start the full stack locally (Kafka + Zookeeper + HDFS + HBase + Ingestion + Streaming + Visualization).
# Streaming connects to the local Kafka — no MSK credentials needed.
# Reads GITHUB_TOKEN from infra/local.env (gitignored).
# Usage: bash infra/dev.sh [extra docker compose flags]
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

ENV_FILE="$DIR/local.env"
if [[ -f "$ENV_FILE" ]]; then
  set -o allexport
  source "$ENV_FILE"
  set +o allexport
elif [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "ERROR: GITHUB_TOKEN is not set. Either create infra/local.env or pass it inline:" >&2
  echo "  GITHUB_TOKEN=<token> bash infra/dev.sh" >&2
  exit 1
fi

echo "==> Building all images..."
bash "$DIR/build-all.sh"

echo ""
echo "==> Starting full dev cluster..."
docker compose -f "$DIR/docker-compose.yml" up -d "$@"

echo ""
echo "==> Cluster is up:"
docker compose -f "$DIR/docker-compose.yml" ps

echo ""
echo "Useful endpoints:"
echo "  Kafka:             localhost:29092"
echo "  HBase Master UI:   http://localhost:16010"
echo "  HDFS NameNode UI:  http://localhost:9870"
echo "  Visualization API: http://localhost:8080"
echo "  Visualization UI:  http://localhost:3000"
echo ""
echo "For frontend hot-reload:  sbt fdev  (serves on :9876)"
echo ""
echo "To stop: docker compose -f infra/docker-compose.yml down"

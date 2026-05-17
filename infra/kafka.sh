#!/usr/bin/env bash
# Start only Kafka + Zookeeper locally for sbt-based ingestion/streaming testing.
# Usage: bash infra/kafka.sh [extra docker compose flags]
#
# Then in separate terminals:
#   sbt p1      — ingestion → local Kafka
#   sbt p2dev   — streaming monitor from local Kafka
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

docker compose -f "$DIR/docker-compose.yml" up -d zookeeper kafka "$@"

echo ""
echo "Kafka ready at localhost:29092"
echo ""
echo "  sbt p1     — run ingestion → local Kafka"
echo "  sbt p2dev  — monitor events from local Kafka"
echo ""
echo "To stop: docker compose -f infra/docker-compose.yml stop zookeeper kafka"

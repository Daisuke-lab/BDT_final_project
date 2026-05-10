#!/usr/bin/env bash
# Start only Kafka + Zookeeper for local development.
# Run ingestion and streaming via sbt instead of Docker.
# Usage: bash infra/dev.sh
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Starting Kafka (+ Zookeeper)..."
docker compose -f "$DIR/docker-compose.yml" up -d zookeeper kafka

echo ""
echo "==> Ready:"
docker compose -f "$DIR/docker-compose.yml" ps zookeeper kafka

echo ""
echo "  Kafka: localhost:29092"
echo ""
echo "  sbt ingestion/run   — fake GitHub event producer"
echo "  sbt streaming/run   — Spark Structured Streaming consumer"
echo ""
echo "To stop: docker compose -f infra/docker-compose.yml down"

#!/usr/bin/env bash
# Start Kafka + Zookeeper + HBase for local development.
# Run ingestion and streaming via sbt instead of Docker.
# Usage: bash infra/dev.sh
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Starting Kafka (+ Zookeeper) + HBase..."
docker compose -f "$DIR/docker-compose.yml" up -d zookeeper kafka hbase

echo ""
echo "==> Ready:"
docker compose -f "$DIR/docker-compose.yml" ps zookeeper kafka hbase

echo ""
echo "  Kafka: localhost:29092"
echo "  HBase: localhost:2182 (Zookeeper client port)"
echo ""
echo "  sbt ingestion/run                                        — fake GitHub event producer"
echo "  sbt streaming/run                                        — Spark Structured Streaming consumer"
echo "  sbt 'streaming/runMain com.bigdata2026.streaming.SeedHBase'  — seed HBase with mock window data"
echo "  sbt vizBackend/run                                       — WebSocket backend on :8080"
echo ""
echo "To stop: docker compose -f infra/docker-compose.yml down"

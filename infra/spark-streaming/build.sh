#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
SBT="$ROOT/sbt"
echo "==> Building spark-streaming fat JAR..."
"$SBT" 'sparkStreaming / assembly'
echo "==> Building image localhost/spark-streaming:latest..."
docker build -f infra/spark-streaming/Dockerfile -t localhost/spark-streaming:latest .
echo "==> Done: localhost/spark-streaming:latest"

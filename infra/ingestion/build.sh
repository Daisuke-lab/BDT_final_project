#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
SBT="$ROOT/sbt"
echo "==> Building ingestion fat JAR..."
"$SBT" ingestion/assembly
echo "==> Building image localhost/ingestion:latest..."
docker build -f infra/ingestion/Dockerfile -t localhost/ingestion:latest .
echo "==> Done: localhost/ingestion:latest"

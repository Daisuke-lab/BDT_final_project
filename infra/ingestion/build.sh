#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
SBT="$ROOT/sbt"
echo "==> Building ingestion fat JAR..."
"$SBT" ingestion/assembly
echo "==> Building image localhost/ingestion:latest (linux/amd64)..."
docker buildx build --platform linux/amd64 -f infra/ingestion/Dockerfile -t localhost/ingestion:latest --load .
echo "==> Done: localhost/ingestion:latest"

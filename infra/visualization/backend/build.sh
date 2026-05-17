#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
SBT="$ROOT/sbt"
echo "==> Building backend fat JAR..."
"$SBT" vizBackend/assembly
echo "==> Building image localhost/visualization-backend:latest (linux/amd64)..."
docker buildx build --platform linux/amd64 -f infra/visualization/backend/Dockerfile -t localhost/visualization-backend:latest --load .
echo "==> Done: localhost/visualization-backend:latest"

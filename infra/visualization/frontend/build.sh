#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
SBT="$ROOT/sbt"

echo "==> Building frontend dist..."
"$SBT" "vizFrontend/publishDist"

echo "==> Building image localhost/visualization-frontend:latest (linux/amd64)..."
docker buildx build --platform linux/amd64 -f infra/visualization/frontend/Dockerfile -t localhost/visualization-frontend:latest --load .
echo "==> Done: localhost/visualization-frontend:latest"

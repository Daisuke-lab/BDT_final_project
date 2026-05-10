#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
SBT="$ROOT/sbt"

echo "==> Building frontend dist..."
"$SBT" "vizFrontend/publishDist"

echo "==> Building image localhost/visualization-frontend:latest..."
docker build -f infra/visualization/frontend/Dockerfile -t localhost/visualization-frontend:latest .
echo "==> Done: localhost/visualization-frontend:latest"

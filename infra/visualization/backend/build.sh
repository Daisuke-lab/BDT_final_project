#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
SBT="$ROOT/sbt"
echo "==> Building backend fat JAR..."
"$SBT" vizBackend/assembly
echo "==> Building image localhost/visualization-backend:latest..."
docker build -f infra/visualization/backend/Dockerfile -t localhost/visualization-backend:latest .
echo "==> Done: localhost/visualization-backend:latest"

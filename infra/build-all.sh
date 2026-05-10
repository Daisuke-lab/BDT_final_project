#!/usr/bin/env bash
# Build all application Docker images.
# Run from anywhere — script resolves the project root automatically.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "======================================="
echo " BigData2026 — build all images"
echo "======================================="

bash "$DIR/ingestion/build.sh"
bash "$DIR/streaming/build.sh"
bash "$DIR/visualization/backend/build.sh"
bash "$DIR/visualization/frontend/build.sh"

echo ""
echo "======================================="
echo " All images ready:"
echo "   localhost/ingestion:latest"
echo "   localhost/streaming:latest"
echo "   localhost/visualization-backend:latest"
echo "   localhost/visualization-frontend:latest"
echo "======================================="

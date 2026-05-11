#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

sbt sparkStreaming/assembly

docker build \
  -f infra/spark-streaming/Dockerfile \
  -t spark-streaming:latest \
  .

#!/usr/bin/env bash
# =============================================================================
#  run_local.sh — Build & run the ingestion producer locally
#
#  Usage:
#    chmod +x ingestion/run_local.sh
#    ./ingestion/run_local.sh                      # uses defaults below
#
#  Override any variable on the command line:
#    GITHUB_TOKEN=ghp_xxx KAFKA_BOOTSTRAP_SERVERS=localhost:29092 ./ingestion/run_local.sh
#
#  Kafka options:
#    Local docker-compose Kafka  →  KAFKA_BOOTSTRAP_SERVERS=localhost:29092  (default)
#    MSK public endpoint         →  set KAFKA_SECURITY_PROTOCOL=SASL_SSL and creds below
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SBT="$ROOT/sbt"

# ── Kafka ─────────────────────────────────────────────────────────────────────
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-github-events}"
export POLL_INTERVAL="${POLL_INTERVAL:-10}"

# ── Load a local secrets file if it exists (never commit this file) ───────────
# Create ingestion/.env.local with your real values, e.g.:
#   GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
#   KAFKA_BOOTSTRAP_SERVERS=b-1-public....amazonaws.com:9196,...
#   KAFKA_SECURITY_PROTOCOL=SASL_SSL
#   KAFKA_SASL_MECHANISM=SCRAM-SHA-512
#   KAFKA_SASL_USERNAME=msk_iot_online_key
#   KAFKA_SASL_PASSWORD=msk_iot_online_secret
ENV_FILE="$(dirname "$0")/.env.local"
if [[ -f "$ENV_FILE" ]]; then
  echo "==> Loading secrets from $ENV_FILE"
  set -o allexport
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +o allexport
fi

# ── Validate required vars ────────────────────────────────────────────────────
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "ERROR: GITHUB_TOKEN is not set."
  echo "  Get one at https://github.com/settings/tokens (no scopes needed)"
  echo "  Then either:"
  echo "    export GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx"
  echo "    or add it to ingestion/.env.local"
  exit 1
fi
# ── Print config ──────────────────────────────────────────────────────────────
echo "============================================="
echo "  BDT Final — Ingestion (local)"
echo "  Kafka:    $KAFKA_BOOTSTRAP_SERVERS → $KAFKA_TOPIC"
echo "  Interval: ${POLL_INTERVAL}s"
echo "  Token:    ${GITHUB_TOKEN:0:10}... (truncated)"
echo "  SASL:     ${KAFKA_SECURITY_PROTOCOL:-off (PLAINTEXT)}"
echo "============================================="
echo ""

# ── Build fat JAR ─────────────────────────────────────────────────────────────
echo "==> Building ingestion fat JAR..."
cd "$ROOT"
"$SBT" ingestion/assembly

JAR="$ROOT/ingestion/target/ingestion-assembly.jar"

if [[ ! -f "$JAR" ]]; then
  echo "ERROR: JAR not found at $JAR"
  exit 1
fi

# ── Run ───────────────────────────────────────────────────────────────────────
echo ""
echo "==> Starting producer (Ctrl+C to stop)..."
echo ""

JAVA_OPTS=(
  -cp "$JAR"
  com.bigdata2026.ingestion.Main
)

# Pass SASL env vars as JVM sys props too (belt-and-suspenders)
exec java "${JAVA_OPTS[@]}"

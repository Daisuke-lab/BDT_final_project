#!/usr/bin/env bash
# =============================================================================
#  push_ecr.sh — Create ECR repo (if needed), build & push ingestion image
#
#  Usage (run from the BDT_final_project root or ingestion/):
#    cd /path/to/BDT_final_project
#    bash ingestion/push_ecr.sh
#
#  Prerequisites:
#    - AWS CLI configured (aws configure)
#    - Docker running
#    - JDK 11 available (for sbt assembly)
# =============================================================================
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
AWS_REGION="us-east-2"
AWS_ACCOUNT="626635437548"
REPO_NAME="bdt-final/ingestion"
IMAGE_TAG="${IMAGE_TAG:-latest}"

ECR_REGISTRY="${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"
FULL_IMAGE="${ECR_REGISTRY}/${REPO_NAME}:${IMAGE_TAG}"

# Resolve project root (works whether you run from root or ingestion/)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ "$(basename "$SCRIPT_DIR")" == "ingestion" ]]; then
  ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
else
  ROOT="$SCRIPT_DIR"
fi
SBT="$ROOT/sbt"

echo "============================================="
echo "  BDT Final — ECR Build & Push"
echo "  Repo:    $FULL_IMAGE"
echo "  Region:  $AWS_REGION"
echo "============================================="
echo ""

# ── 1. Create ECR repository if it doesn't exist ─────────────────────────────
echo "==> Checking ECR repository: $REPO_NAME ..."
if aws ecr describe-repositories \
     --repository-names "$REPO_NAME" \
     --region "$AWS_REGION" \
     --output text > /dev/null 2>&1; then
  echo "    Repository already exists — skipping creation."
else
  echo "    Not found. Creating repository..."
  aws ecr create-repository \
    --repository-name "$REPO_NAME" \
    --region "$AWS_REGION" \
    --image-scanning-configuration scanOnPush=true \
    --output table
  echo "    Repository created."
fi
echo ""

# ── 2. Build fat JAR via sbt ──────────────────────────────────────────────────
echo "==> Building ingestion fat JAR (sbt assembly)..."
cd "$ROOT"
"$SBT" ingestion/assembly

JAR="$ROOT/ingestion/target/ingestion-assembly.jar"
if [[ ! -f "$JAR" ]]; then
  echo "ERROR: JAR not found at $JAR — sbt assembly may have failed."
  exit 1
fi
echo "    JAR ready: $JAR"
echo ""

# ── 3. Build Docker image ─────────────────────────────────────────────────────
# Build for linux/amd64 explicitly — Kubernetes nodes are x86_64 even on Apple Silicon Macs
echo "==> Building Docker image (linux/amd64): $FULL_IMAGE ..."
GIT_SHA=$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo "local")
docker buildx build \
  --platform linux/amd64 \
  -f "$ROOT/infra/ingestion/Dockerfile" \
  -t "$FULL_IMAGE" \
  -t "${ECR_REGISTRY}/${REPO_NAME}:${GIT_SHA}" \
  --push \
  "$ROOT"
echo ""

# ── 4. Authenticate Docker to ECR ────────────────────────────────────────────
echo "==> Logging in to ECR..."
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REGISTRY"
echo ""

# ── 5. Push image ─────────────────────────────────────────────────────────────
# Image was already pushed by `docker buildx build --push` above.
GIT_SHA=$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo "")
echo ""

echo "============================================="
echo "  Done!"
echo "  Image: $FULL_IMAGE"
echo ""
echo "  Deploy to Kubernetes:"
echo "    kubectl apply -f ingestion/secret.yaml"
echo "    kubectl apply -f ingestion/pod.yaml"
echo ""
echo "  Watch logs:"
echo "    kubectl logs -f pod/ingestion"
echo "============================================="

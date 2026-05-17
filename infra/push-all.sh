#!/usr/bin/env bash
# =============================================================================
#  push-all.sh — Push all pre-built application images to ECR
#
#  Usage (run from the BDT_final_project root or infra/):
#    bash infra/build-all.sh   # build images first
#    bash infra/push-all.sh    # then push to ECR
#
#  Override the tag:
#    IMAGE_TAG=v1.2.3 bash infra/push-all.sh
#
#  Prerequisites:
#    - AWS CLI configured (aws configure)
#    - Docker running with buildx
# =============================================================================
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
AWS_REGION="us-east-2"
AWS_ACCOUNT="626635437548"
IMAGE_TAG="${IMAGE_TAG:-latest}"

ECR_REGISTRY="${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Resolve project root (works whether you run from root or infra/)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ "$(basename "$SCRIPT_DIR")" == "infra" ]]; then
  ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
else
  ROOT="$SCRIPT_DIR"
fi
GIT_SHA=$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo "local")

# ── Module definitions: "local-image|repo-name|dockerfile-path" ──────────────
# local-image: image name produced by build-all.sh (localhost/...)
# repo-name:   ECR repository name
# dockerfile:  path relative to project root
declare -a MODULES=(
  "localhost/ingestion:latest|bdt-final/ingestion|infra/ingestion/Dockerfile"
  "localhost/spark-streaming:latest|bdt-final/spark-streaming|infra/spark-streaming/Dockerfile"
  "localhost/visualization-backend:latest|bdt-final/visualization-backend|infra/visualization/backend/Dockerfile"
  "localhost/visualization-frontend:latest|bdt-final/visualization-frontend|infra/visualization/frontend/Dockerfile"
)

echo "============================================="
echo "  BDT Final — Push All Images to ECR"
echo "  Registry: $ECR_REGISTRY"
echo "  Tag:      $IMAGE_TAG  ($GIT_SHA)"
echo "============================================="
echo ""

# ── 1. Authenticate Docker to ECR (once, upfront) ────────────────────────────
echo "==> Logging in to ECR..."
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REGISTRY"
echo ""

# ── 2. Tag & push each image ─────────────────────────────────────────────────
for MODULE in "${MODULES[@]}"; do
  IFS='|' read -r LOCAL_IMAGE REPO_NAME DOCKERFILE <<< "$MODULE"
  FULL_IMAGE="${ECR_REGISTRY}/${REPO_NAME}:${IMAGE_TAG}"

  echo "---------------------------------------------"
  echo "  Module:     $REPO_NAME"
  echo "  Image:      $FULL_IMAGE"
  echo "---------------------------------------------"

  # Ensure ECR repository exists
  if aws ecr describe-repositories \
       --repository-names "$REPO_NAME" \
       --region "$AWS_REGION" \
       --output text > /dev/null 2>&1; then
    echo "  ECR repo already exists — skipping creation."
  else
    echo "  Creating ECR repo: $REPO_NAME ..."
    aws ecr create-repository \
      --repository-name "$REPO_NAME" \
      --region "$AWS_REGION" \
      --image-scanning-configuration scanOnPush=true \
      --output table
  fi

  # Tag the locally-built image and push to ECR
  echo "  Tagging & pushing..."
  docker tag "$LOCAL_IMAGE" "$FULL_IMAGE"
  docker tag "$LOCAL_IMAGE" "${ECR_REGISTRY}/${REPO_NAME}:${GIT_SHA}"
  docker push "$FULL_IMAGE"
  docker push "${ECR_REGISTRY}/${REPO_NAME}:${GIT_SHA}"
  echo "  Pushed: $FULL_IMAGE"
  echo ""
done

# ── 4. Summary ────────────────────────────────────────────────────────────────
echo "============================================="
echo "  All images pushed!"
echo ""
for MODULE in "${MODULES[@]}"; do
  IFS='|' read -r _ REPO_NAME _ <<< "$MODULE"
  echo "    ${ECR_REGISTRY}/${REPO_NAME}:${IMAGE_TAG}"
done
echo ""
echo "  Git SHA tag: $GIT_SHA"
echo "============================================="

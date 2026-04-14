#!/bin/bash
set -euo pipefail

TAG="${1:-latest}"
cd "$(dirname "$0")/.."

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || true)}"

if [[ -z "$REGION" ]]; then
    echo "Error: no AWS region set. Set AWS_REGION env var or run 'aws configure'." >&2
    exit 1
fi

ECR_HOST="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"
IMAGE="$ECR_HOST/foodatlas-db"

aws ecr get-login-password --region "$REGION" \
    | docker login --username AWS --password-stdin "$ECR_HOST"

docker build --platform linux/amd64 -t "foodatlas-db:$TAG" .
docker tag "foodatlas-db:$TAG" "$IMAGE:$TAG"
docker push "$IMAGE:$TAG"

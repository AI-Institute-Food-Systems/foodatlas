#!/bin/bash
set -euo pipefail

TAG="${1:-latest}"
cd "$(dirname "$0")/.."

aws ecr get-login-password --region us-west-1 \
    | docker login --username AWS --password-stdin 030635937737.dkr.ecr.us-west-1.amazonaws.com

docker build --platform linux/amd64 -t foodatlas-db:"$TAG" .
docker tag foodatlas-db:"$TAG" 030635937737.dkr.ecr.us-west-1.amazonaws.com/foodatlas-db:"$TAG"
docker push 030635937737.dkr.ecr.us-west-1.amazonaws.com/foodatlas-db:"$TAG"

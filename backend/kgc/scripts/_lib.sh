#!/bin/bash
# Shared helpers for KGC sync/pull scripts. Sourced by sibling scripts.

set -euo pipefail

REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || true)}"
if [[ -z "$REGION" ]]; then
    echo "Error: no AWS region set. Set AWS_REGION env var or run 'aws configure'." >&2
    exit 1
fi

resolve_kgc_bucket() {
    BUCKET=$(aws cloudformation describe-stacks \
        --stack-name FoodAtlasStorageStack \
        --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='KgcBucketName'].OutputValue" \
        --output text)

    if [[ -z "$BUCKET" || "$BUCKET" == "None" ]]; then
        echo "Error: could not resolve KgcBucketName from FoodAtlasStorageStack." >&2
        exit 1
    fi
}

utc_timestamp() {
    date -u +"%Y%m%dT%H%M%SZ"
}

read_latest() {
    # Args: $1 = prefix (e.g. "data" or "outputs")
    local prefix="$1"
    aws s3 cp "s3://$BUCKET/$prefix/LATEST" - --region "$REGION" 2>/dev/null || true
}

#!/bin/bash
# Publish the local KGC parquet outputs to S3 as an immutable, versioned run.
#
# Each invocation creates a new directory under s3://<bucket>/kg/<UTC-ts>/
# and updates the s3://<bucket>/kg/LATEST pointer file. Old runs are never
# touched, so historical versions remain available for rollback or diffing.
#
# Usage: ./sync-to-s3.sh

set -euo pipefail

REGION="${AWS_REGION:-us-west-1}"
cd "$(dirname "$0")/.."

LOCAL_DIR="outputs/kg"

if [[ ! -d "$LOCAL_DIR" ]]; then
    echo "Error: $LOCAL_DIR does not exist. Run the KGC pipeline first." >&2
    exit 1
fi

if [[ -z "$(ls -A "$LOCAL_DIR" 2>/dev/null || true)" ]]; then
    echo "Error: $LOCAL_DIR is empty. Nothing to upload." >&2
    exit 1
fi

BUCKET=$(aws cloudformation describe-stacks \
    --stack-name FoodAtlasStorageStack \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='ParquetBucketName'].OutputValue" \
    --output text)

if [[ -z "$BUCKET" || "$BUCKET" == "None" ]]; then
    echo "Error: could not resolve ParquetBucketName from FoodAtlasStorageStack." >&2
    exit 1
fi

VERSION=$(date -u +"%Y%m%dT%H%M%SZ")
DEST="s3://$BUCKET/kg/$VERSION/"

echo "Uploading $LOCAL_DIR -> $DEST"
aws s3 sync "$LOCAL_DIR/" "$DEST" --region "$REGION"

echo "Updating LATEST pointer to $VERSION"
echo -n "$VERSION" | aws s3 cp - "s3://$BUCKET/kg/LATEST" --region "$REGION"

echo
echo "Done. KGC version: $VERSION"
echo "Load this version into RDS with:"
echo "  cd infra/cdk && ./scripts/run-data-load.sh"
echo "or pin a specific version with:"
echo "  cd infra/cdk && ./scripts/run-data-load.sh $VERSION"

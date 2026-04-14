#!/bin/bash
# Run the ETL data loader against RDS via a one-off Fargate task.
#
# Usage: ./run-data-load.sh [version]
#   version: KGC version timestamp (e.g. 20260413T221503Z) to load.
#            With no argument, reads s3://<bucket>/kg/LATEST and loads
#            whichever version it points at. Pass an explicit version to
#            roll back to or pin a specific KGC run.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=_lib.sh
source ./_lib.sh

REQUESTED_VERSION="${1:-}"

BUCKET=$(aws cloudformation describe-stacks \
    --stack-name FoodAtlasStorageStack \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='ParquetBucketName'].OutputValue" \
    --output text)

if [[ -z "$BUCKET" || "$BUCKET" == "None" ]]; then
    echo "Error: could not resolve ParquetBucketName from FoodAtlasStorageStack." >&2
    exit 1
fi

if [[ -n "$REQUESTED_VERSION" ]]; then
    VERSION="$REQUESTED_VERSION"
    echo "Loading explicitly requested version: $VERSION"
else
    echo "Reading s3://$BUCKET/kg/LATEST..."
    VERSION=$(aws s3 cp "s3://$BUCKET/kg/LATEST" - --region "$REGION" 2>/dev/null || true)
    if [[ -z "$VERSION" ]]; then
        echo "Error: s3://$BUCKET/kg/LATEST is missing or empty." >&2
        echo "Run backend/kgc/scripts/sync-to-s3.sh first to publish a KGC version." >&2
        exit 1
    fi
    echo "LATEST -> $VERSION"
fi

PARQUET_DIR="s3://$BUCKET/kg/$VERSION/"

# Sanity check: the version directory must actually contain objects.
if ! aws s3 ls "$PARQUET_DIR" --region "$REGION" >/dev/null 2>&1; then
    echo "Error: $PARQUET_DIR does not exist or is empty." >&2
    exit 1
fi

COMMAND_JSON="[\"uv\",\"run\",\"python\",\"main.py\",\"load\",\"--parquet-dir\",\"$PARQUET_DIR\"]"
run_jobs_task "$COMMAND_JSON" "data load from $PARQUET_DIR"

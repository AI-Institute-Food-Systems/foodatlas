#!/bin/bash
# Download a versioned KGC parquet run from S3 into the local PreviousFAKG
# baseline folder. Inverse of sync-to-s3.sh.
#
# Usage: ./pull-from-s3.sh [version]
#   version: KGC version timestamp (e.g. 20260413T221503Z) to download.
#            With no argument, reads s3://<bucket>/kg/LATEST and pulls
#            whichever version it points at.
#
# Files land at: backend/kgc/data/PreviousFAKG/<version>/

set -euo pipefail

REGION="${AWS_REGION:-us-west-1}"
cd "$(dirname "$0")/.."

DEST_ROOT="data/PreviousFAKG"

BUCKET=$(aws cloudformation describe-stacks \
    --stack-name FoodAtlasStorageStack \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='ParquetBucketName'].OutputValue" \
    --output text)

if [[ -z "$BUCKET" || "$BUCKET" == "None" ]]; then
    echo "Error: could not resolve ParquetBucketName from FoodAtlasStorageStack." >&2
    exit 1
fi

REQUESTED_VERSION="${1:-}"

if [[ -n "$REQUESTED_VERSION" ]]; then
    VERSION="$REQUESTED_VERSION"
    echo "Pulling explicitly requested version: $VERSION"
else
    echo "Reading s3://$BUCKET/kg/LATEST..."
    VERSION=$(aws s3 cp "s3://$BUCKET/kg/LATEST" - --region "$REGION" 2>/dev/null || true)
    if [[ -z "$VERSION" ]]; then
        echo "Error: s3://$BUCKET/kg/LATEST is missing or empty." >&2
        echo "Run sync-to-s3.sh first to publish a KGC version." >&2
        exit 1
    fi
    echo "LATEST -> $VERSION"
fi

SRC="s3://$BUCKET/kg/$VERSION/"

if ! aws s3 ls "$SRC" --region "$REGION" >/dev/null 2>&1; then
    echo "Error: $SRC does not exist or is empty." >&2
    exit 1
fi

DEST="$DEST_ROOT/$VERSION"
mkdir -p "$DEST"

echo "Downloading $SRC -> $DEST/"
aws s3 sync "$SRC" "$DEST/" --region "$REGION"

echo
echo "Done. Local copy: $DEST/"

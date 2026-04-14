#!/bin/bash
# Publish the local KGC source data tree to S3 as an immutable, versioned
# snapshot. Uploads everything under backend/kgc/data/ EXCEPT PreviousFAKG/
# (which is itself derived from a previous KGC outputs run).
#
# Each invocation creates a new directory under s3://<bucket>/data/<UTC-ts>/
# and updates s3://<bucket>/data/LATEST. Source ontologies are large and
# refresh rarely (quarterly), so this script is run only when a registry
# (FoodOn, ChEBI, CTD, etc.) actually publishes a new version.
#
# Usage: ./sync-data-to-s3.sh

set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=_lib.sh
source ./scripts/_lib.sh

LOCAL_DIR="data"

if [[ ! -d "$LOCAL_DIR" ]]; then
    echo "Error: $LOCAL_DIR does not exist." >&2
    exit 1
fi

if [[ -z "$(ls -A "$LOCAL_DIR" 2>/dev/null || true)" ]]; then
    echo "Error: $LOCAL_DIR is empty. Nothing to upload." >&2
    exit 1
fi

resolve_kgc_bucket
VERSION=$(utc_timestamp)
DEST="s3://$BUCKET/data/$VERSION/"

echo "Uploading $LOCAL_DIR/ -> $DEST (excluding PreviousFAKG/, Lit2KG/, and repo housekeeping)"
aws s3 sync "$LOCAL_DIR/" "$DEST" \
    --region "$REGION" \
    --exclude "PreviousFAKG/*" \
    --exclude "Lit2KG/*" \
    --exclude "*README.md" \
    --exclude "*.gitignore" \
    --exclude "*download.sh"

echo "Updating data/LATEST -> $VERSION"
echo -n "$VERSION" | aws s3 cp - "s3://$BUCKET/data/LATEST" --region "$REGION"

echo
echo "Done. KGC data version: $VERSION"

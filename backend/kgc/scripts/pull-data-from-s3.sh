#!/bin/bash
# Download a versioned KGC source data snapshot from S3 into the local
# backend/kgc/data/ tree. Inverse of sync-data-to-s3.sh.
#
# Usage: ./pull-data-from-s3.sh [version]
#   version: data version timestamp (e.g. 20260413T221503Z) to download.
#            With no argument, reads s3://<bucket>/data/LATEST and pulls
#            whichever version it points at.
#
# Files land directly under backend/kgc/data/, overlaying any existing
# files. PreviousFAKG/ is not touched (it is managed by pull-from-s3.sh).

set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=_lib.sh
source ./scripts/_lib.sh

DEST="data"

resolve_kgc_bucket

REQUESTED_VERSION="${1:-}"

if [[ -n "$REQUESTED_VERSION" ]]; then
    VERSION="$REQUESTED_VERSION"
    echo "Pulling explicitly requested version: $VERSION"
else
    echo "Reading s3://$BUCKET/data/LATEST..."
    VERSION=$(read_latest data)
    if [[ -z "$VERSION" ]]; then
        echo "Error: s3://$BUCKET/data/LATEST is missing or empty." >&2
        echo "Run sync-data-to-s3.sh first to publish a KGC data version." >&2
        exit 1
    fi
    echo "data/LATEST -> $VERSION"
fi

SRC="s3://$BUCKET/data/$VERSION/"

if ! aws s3 ls "$SRC" --region "$REGION" >/dev/null 2>&1; then
    echo "Error: $SRC does not exist or is empty." >&2
    exit 1
fi

mkdir -p "$DEST"

echo "Downloading $SRC -> $DEST/ (PreviousFAKG/ untouched)"
aws s3 sync "$SRC" "$DEST/" --region "$REGION"

echo
echo "Done. Local data version: $VERSION"

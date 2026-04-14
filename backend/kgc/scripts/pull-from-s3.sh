#!/bin/bash
# Download a versioned KGC outputs run from S3 into the local PreviousFAKG
# baseline folder. Pulls only the loadable kg/ subset (parquet files), not
# the diagnostics/checkpoints/intermediate/ingest sidecars.
#
# Usage: ./pull-from-s3.sh [version]
#   version: KGC outputs version timestamp (e.g. 20260413T221503Z) to download.
#            With no argument, reads s3://<bucket>/outputs/LATEST and pulls
#            whichever version it points at.
#
# Files land at: backend/kgc/data/PreviousFAKG/<version>/

set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=_lib.sh
source ./scripts/_lib.sh

DEST_ROOT="data/PreviousFAKG"

resolve_kgc_bucket

REQUESTED_VERSION="${1:-}"

if [[ -n "$REQUESTED_VERSION" ]]; then
    VERSION="$REQUESTED_VERSION"
    echo "Pulling explicitly requested version: $VERSION"
else
    echo "Reading s3://$BUCKET/outputs/LATEST..."
    VERSION=$(read_latest outputs)
    if [[ -z "$VERSION" ]]; then
        echo "Error: s3://$BUCKET/outputs/LATEST is missing or empty." >&2
        echo "Run sync-outputs-to-s3.sh first to publish a KGC outputs version." >&2
        exit 1
    fi
    echo "outputs/LATEST -> $VERSION"
fi

SRC="s3://$BUCKET/outputs/$VERSION/kg/"

if ! aws s3 ls "$SRC" --region "$REGION" >/dev/null 2>&1; then
    echo "Error: $SRC does not exist or is empty." >&2
    exit 1
fi

DEST="$DEST_ROOT/$VERSION"
mkdir -p "$DEST"

echo "Downloading $SRC -> $DEST/"
aws s3 sync "$SRC" "$DEST/" \
    --region "$REGION" \
    --exclude "checkpoints/*" \
    --exclude "diagnostics/*" \
    --exclude "intermediate/*"

echo
echo "Done. Local PreviousFAKG baseline: $DEST/"

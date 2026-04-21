#!/bin/bash
# Publish the local KGC pipeline output tree to S3 as an immutable, versioned
# run. Uploads everything under backend/kgc/outputs/ — including kg/ (the
# loadable parquet files), checkpoints/, diagnostics/, intermediate/, and the
# per-source ingest/ folder.
#
# Each invocation creates a new directory under s3://<bucket>/outputs/<UTC-ts>/
# and updates s3://<bucket>/outputs/LATEST. A manifest.json is written at the
# root of the version directory recording the data version, git SHA, host,
# and user that produced this run.
#
# Usage: ./sync-outputs-to-s3.sh

set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=_lib.sh
source ./scripts/_lib.sh

LOCAL_DIR="outputs"

if [[ ! -d "$LOCAL_DIR" ]]; then
    echo "Error: $LOCAL_DIR does not exist. Run the KGC pipeline first." >&2
    exit 1
fi

if [[ -z "$(ls -A "$LOCAL_DIR" 2>/dev/null || true)" ]]; then
    echo "Error: $LOCAL_DIR is empty. Nothing to upload." >&2
    exit 1
fi

if [[ ! -f "$LOCAL_DIR/kg/CHANGELOG.md" ]]; then
    cat >&2 <<MISSING
Error: $LOCAL_DIR/kg/CHANGELOG.md not found.

Generate it before syncing so every KGC run on S3 carries a release
report that downstream publishes can reference:

    uv run python main.py report
MISSING
    exit 1
fi

resolve_kgc_bucket
VERSION=$(utc_timestamp)
DEST="s3://$BUCKET/outputs/$VERSION/"

echo "Uploading $LOCAL_DIR/ -> $DEST (excluding repo housekeeping)"
aws s3 sync "$LOCAL_DIR/" "$DEST" \
    --region "$REGION" \
    --exclude "*README.md" \
    --exclude "*.gitignore" \
    --exclude "*download.sh"

# Build the manifest file referencing the data version that was current at
# upload time. If no data has been published yet, data_version is null.
DATA_VERSION=$(read_latest data)
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
HOSTNAME_VALUE=$(hostname)
USER_VALUE="${USER:-unknown}"

MANIFEST=$(cat <<EOF
{
  "version": "$VERSION",
  "created_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "data_version": $( [[ -n "$DATA_VERSION" ]] && echo "\"$DATA_VERSION\"" || echo "null" ),
  "git_sha": "$GIT_SHA",
  "git_branch": "$GIT_BRANCH",
  "host": "$HOSTNAME_VALUE",
  "user": "$USER_VALUE"
}
EOF
)

echo "Writing manifest.json"
echo "$MANIFEST" | aws s3 cp - "${DEST}manifest.json" --region "$REGION"

echo "Updating outputs/LATEST -> $VERSION"
echo -n "$VERSION" | aws s3 cp - "s3://$BUCKET/outputs/LATEST" --region "$REGION"

echo
echo "Done. KGC outputs version: $VERSION"
echo "Load this version into RDS with:"
echo "  cd infra/aws && ./scripts/run-data-load.sh"
echo "or pin a specific version with:"
echo "  cd infra/aws && ./scripts/run-data-load.sh $VERSION"

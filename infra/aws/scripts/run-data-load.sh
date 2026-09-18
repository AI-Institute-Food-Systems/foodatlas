#!/bin/bash
# Run the ETL data loader against RDS via a one-off Fargate task.
#
# Usage: ./run-data-load.sh [version]
#   version: KGC outputs version timestamp (e.g. 20260413T221503Z) to load.
#            With no argument, reads s3://<bucket>/outputs/LATEST and loads
#            whichever version it points at. Pass an explicit version to
#            roll back to or pin a specific KGC run.
#   --allow-no-bioactivity: load a version whose kg/ lacks the bioactivity
#            parquet. Refused by default — prod must always carry
#            bioactivity, and raw KGC injection runs don't (see
#            infra/README.md "Load a new KGC run onto prod").

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=_lib.sh
source ./_lib.sh

DRY_RUN=""
ALLOW_NO_BIOACTIVITY=""
while [[ "${1:-}" == --* ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1 ;;
        --allow-no-bioactivity) ALLOW_NO_BIOACTIVITY=1 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
    shift
done
REQUESTED_VERSION="${1:-}"

BUCKET=$(aws cloudformation describe-stacks \
    --stack-name FoodAtlasStorageStack \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='KgcBucketName'].OutputValue" \
    --output text)

if [[ -z "$BUCKET" || "$BUCKET" == "None" ]]; then
    echo "Error: could not resolve KgcBucketName from FoodAtlasStorageStack." >&2
    exit 1
fi

if [[ -n "$REQUESTED_VERSION" ]]; then
    VERSION="$REQUESTED_VERSION"
    echo "Loading explicitly requested version: $VERSION"
else
    echo "Reading s3://$BUCKET/outputs/LATEST..."
    VERSION=$(aws s3 cp "s3://$BUCKET/outputs/LATEST" - --region "$REGION" 2>/dev/null || true)
    if [[ -z "$VERSION" ]]; then
        echo "Error: s3://$BUCKET/outputs/LATEST is missing or empty." >&2
        echo "Run backend/kgc/scripts/sync-outputs-to-s3.sh first to publish a KGC outputs version." >&2
        exit 1
    fi
    echo "outputs/LATEST -> $VERSION"
fi

PARQUET_DIR="s3://$BUCKET/outputs/$VERSION/kg/"

# Sanity check: the version's kg/ directory must actually contain objects.
if ! aws s3 ls "$PARQUET_DIR" --region "$REGION" >/dev/null 2>&1; then
    echo "Error: $PARQUET_DIR does not exist or is empty." >&2
    exit 1
fi

# Raw KGC injection runs lack the bioactivity parquet and the loader treats
# it as optional, so loading one silently drops bioactivity from prod.
# Merge first (backend/kgc/scripts/merge_bioactivity_delta.py), then load.
if ! aws s3 ls "${PARQUET_DIR}attestations_bioactivity.parquet" --region "$REGION" >/dev/null 2>&1; then
    if [[ -z "$ALLOW_NO_BIOACTIVITY" ]]; then
        cat >&2 <<NOBIO
Error: $PARQUET_DIR has no attestations_bioactivity.parquet.

Loading it would drop bioactivity from prod. Merge the bioactivity delta
onto this run first (infra/README.md → "Load a new KGC run onto prod") and
load the merged version. To load anyway: --allow-no-bioactivity
NOBIO
        exit 1
    fi
    echo "WARNING: no bioactivity parquet in $PARQUET_DIR — loading anyway (--allow-no-bioactivity)." >&2
fi

COMMAND_JSON="[\"python\",\"main.py\",\"load\",\"--parquet-dir\",\"$PARQUET_DIR\"]"
if [[ -n "$DRY_RUN" ]]; then
    echo "[dry-run] would launch Fargate jobs task (NOT launched):"
    echo "[dry-run]   loads:   $PARQUET_DIR"
    echo "[dry-run]   command: $COMMAND_JSON"
else
    run_jobs_task "$COMMAND_JSON" "data load from $PARQUET_DIR"
fi

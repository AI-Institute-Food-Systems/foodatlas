#!/bin/bash
# Run the ETL data loader against RDS via a one-off Fargate task.
#
# Usage: ./run-data-load.sh [version]
#   version: KGC outputs version timestamp (e.g. 20260413T221503Z) to load.
#            With no argument, reads s3://<bucket>/outputs/LATEST and loads
#            whichever version it points at. Pass an explicit version to
#            roll back to or pin a specific KGC run.
#   --allow-no-bioactivity: load a version whose kg/ is missing any of the
#            five bioactivity parquet files. Refused by default — prod must
#            always carry bioactivity, and the DB loader treats every one of
#            those files as optional, so a partial run loads clean and
#            silently drops data. Use only to roll back to a pre-bioactivity
#            run (see infra/README.md "Load a new KGC run onto prod").

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

# backend/db treats every bioactivity table as optional, so a run missing any
# of them loads clean and silently drops that slice of prod. A run built with
# the bioactivity source emits all five; check them all rather than only
# attestations_bioactivity, because a partially-complete run is the failure
# mode that hides.
BIOACTIVITY_PARQUET=(
    attestations_bioactivity.parquet
    bioassays.parquet
    food_chemical_efficacy.parquet
    bioactivity_disease.parquet
    bioactivity_disease_targets.parquet
)
MISSING_PARQUET=()
for f in "${BIOACTIVITY_PARQUET[@]}"; do
    aws s3 ls "${PARQUET_DIR}${f}" --region "$REGION" >/dev/null 2>&1 \
        || MISSING_PARQUET+=("$f")
done

if [[ ${#MISSING_PARQUET[@]} -gt 0 ]]; then
    if [[ -z "$ALLOW_NO_BIOACTIVITY" ]]; then
        cat >&2 <<NOBIO
Error: $PARQUET_DIR is missing ${#MISSING_PARQUET[@]} of ${#BIOACTIVITY_PARQUET[@]} bioactivity parquet files:
$(printf '  - %s\n' "${MISSING_PARQUET[@]}")
Loading it would drop that data from prod. Build the run with the bioactivity
source enabled (it emits all five), or merge the delta onto this run first
(infra/README.md → "Load a new KGC run onto prod"), then load the result.
To load anyway: --allow-no-bioactivity
NOBIO
        exit 1
    fi
    echo "WARNING: $PARQUET_DIR is missing ${MISSING_PARQUET[*]} —" \
         "loading anyway (--allow-no-bioactivity)." >&2
fi

COMMAND_JSON="[\"python\",\"main.py\",\"load\",\"--parquet-dir\",\"$PARQUET_DIR\"]"
if [[ -n "$DRY_RUN" ]]; then
    echo "[dry-run] would launch Fargate jobs task (NOT launched):"
    echo "[dry-run]   loads:   $PARQUET_DIR"
    echo "[dry-run]   command: $COMMAND_JSON"
else
    run_jobs_task "$COMMAND_JSON" "data load from $PARQUET_DIR"
fi

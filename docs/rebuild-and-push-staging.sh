#!/usr/bin/env bash
# Rebuild the bioactivity KG bundle and push it to the STAGING RDS + API.
#
# One script for the whole staging deploy of backend changes:
#   1. (optional) rebuild the KG via the KGC pipeline
#   2. build + push the db/api Docker images (tag `bioactivity`)
#   3. sync the KG parquet bundle to the staging S3 prefix
#   4. run the staging ETL Fargate task  -> loads the STAGING RDS + rebuilds MVs
#   5. force a new deployment of the staging API service
#
# It ONLY ever touches staging: the `bioactivity` image tag, the
# `outputs/staging-bioactivity/` S3 prefix, and the `*-Staging` stacks. Prod uses
# pinned SHA tags, `outputs/LATEST`, and the un-suffixed stacks — never touched here.
#
# WHICH RDS GETS WRITTEN (read this):
#   The ETL never takes a DB argument. The `db load` command builds its connection
#   from DBSettings (backend/db/src/config.py) -> DB_HOST/DB_PORT/DB_NAME/DB_USER/
#   DB_PASSWORD. In AWS those vars are injected into the Fargate task's container
#   from the RDS credentials secret wired in the CDK Jobs stack. So "which RDS" is
#   decided by WHICH JOBS STACK the task runs against:
#       JOBS_STACK=FoodAtlasJobsStack          -> production RDS
#       JOBS_STACK=FoodAtlasJobsStack-Staging   -> staging RDS   (default below)
#   That selection happens via the STACK variable in infra/aws/scripts/_lib.sh,
#   which this script overrides to the staging stack before launching the task.
#
# Prereqs: aws sso login --profile <profile>; docker; run from anywhere.
# Usage:   AWS_PROFILE=fa_kaichi ./docs/rebuild-and-push-staging.sh [--dry-run]

set -euo pipefail

# ---- config (override via env) ---------------------------------------------
REGION="${AWS_REGION:-us-west-1}"
ECR="${ECR:-030635937737.dkr.ecr.us-west-1.amazonaws.com}"
IMAGE_TAG="${IMAGE_TAG:-bioactivity}"
KGC_BUCKET="${KGC_BUCKET:-foodatlasstoragestack-kgcbucket77ae180e-fu9ow2rwr4u1}"
S3_PREFIX="${S3_PREFIX:-outputs/staging-bioactivity/kg/}"
JOBS_STACK="${JOBS_STACK:-FoodAtlasJobsStack-Staging}"
API_CLUSTER="${API_CLUSTER:-FoodAtlasApiStack-Staging-ApiCluster7CE9CBE6-BtVAz1IpCkmx}"
API_SERVICE="${API_SERVICE:-FoodAtlasApiStack-Staging-ApiService199661B5-QAbwy3lSlebq}"

# KG bundle to ship. REBUILD_KG modes:
#   none  (default) ship KG_DIR (backend/kgc/outputs/kg) as-is
#   full  from-scratch LLM-free build (stages ingest:enrichment) -> outputs/kg,
#         then ship that. Reproduces v4.3 from the IE inputs in ../ie/outputs.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KG_DIR="${KG_DIR:-$REPO_ROOT/backend/kgc/outputs/kg}"
REBUILD_KG="${REBUILD_KG:-none}"

DRY_RUN=""
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

# ---- guards: never touch production ----------------------------------------
assert_staging_only() {
    [[ "$JOBS_STACK" == *Staging* ]] || { echo "REFUSE: JOBS_STACK is not a -Staging stack ($JOBS_STACK)" >&2; exit 1; }
    [[ "$S3_PREFIX" == *staging* ]]  || { echo "REFUSE: S3_PREFIX is not a staging prefix ($S3_PREFIX)" >&2; exit 1; }
    [[ "$IMAGE_TAG" != "latest" ]]   || { echo "REFUSE: IMAGE_TAG=latest is a production tag" >&2; exit 1; }
    aws sts get-caller-identity --region "$REGION" >/dev/null \
        || { echo "Not authenticated. Run: aws sso login --profile <profile>" >&2; exit 1; }
}

run() { if [[ -n "$DRY_RUN" ]]; then echo "[dry-run] $*"; else echo "+ $*"; "$@"; fi; }

# ---- steps -----------------------------------------------------------------
rebuild_kg() {
    case "$REBUILD_KG" in
        none | 0 | "")
            echo "KG rebuild: none — shipping $KG_DIR"
            ;;
        full)
            echo "== KG rebuild: FULL pipeline (stages ingest:enrichment, LLM-free) =="
            # Stages 0-4 build the complete loadable KG (incl. lit2kg composition
            # from ../ie/outputs/extraction and the bioactivity layer). TRUST/
            # EVALUATION/NEWSLETTER (5-7) are LLM stages and deliberately skipped.
            # --config run_config.json pins entity-id carry-forward for stable ids.
            run bash -c "cd '$REPO_ROOT/backend/kgc' && uv run python main.py --config run_config.json run --stages ingest:enrichment"
            KG_DIR="$REPO_ROOT/backend/kgc/outputs/kg"
            ;;
        *)
            echo "REFUSE: unknown REBUILD_KG='$REBUILD_KG' (use: none | full)" >&2
            exit 1
            ;;
    esac
}

login_ecr() {
    echo "== ECR login =="
    if [[ -z "$DRY_RUN" ]]; then
        aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ECR"
    fi
}

build_and_push_images() {
    echo "== Build + push db/api images (tag $IMAGE_TAG) =="
    run docker build -t "$ECR/foodatlas-db:$IMAGE_TAG" "$REPO_ROOT/backend/db"
    run docker push  "$ECR/foodatlas-db:$IMAGE_TAG"
    run docker build -t "$ECR/foodatlas-api:$IMAGE_TAG" "$REPO_ROOT/backend/api"
    run docker push  "$ECR/foodatlas-api:$IMAGE_TAG"
}

sync_kg_to_staging() {
    echo "== Sync KG bundle -> s3://$KGC_BUCKET/$S3_PREFIX =="
    for f in entities.parquet triplets.parquet attestations_bioactivity.parquet; do
        [[ -f "$KG_DIR/$f" ]] || { echo "REFUSE: $KG_DIR missing $f — not a complete KG bundle." >&2; exit 1; }
    done
    [[ -f "$KG_DIR/bioassays.parquet" ]] || echo "WARNING: $KG_DIR has no bioassays.parquet (base_bioassays will be empty)" >&2
    run aws s3 sync "$KG_DIR/" "s3://$KGC_BUCKET/$S3_PREFIX" --delete --only-show-errors --region "$REGION"
}

run_staging_etl() {
    echo "== Run staging ETL (loads STAGING RDS via $JOBS_STACK) =="
    # shellcheck source=../infra/aws/scripts/_lib.sh
    source "$REPO_ROOT/infra/aws/scripts/_lib.sh"
    STACK="$JOBS_STACK"   # override _lib.sh default (FoodAtlasJobsStack) -> staging
    local cmd="[\"python\",\"main.py\",\"load\",\"--parquet-dir\",\"s3://$KGC_BUCKET/$S3_PREFIX\"]"
    if [[ -n "$DRY_RUN" ]]; then echo "[dry-run] run_jobs_task $cmd"; else run_jobs_task "$cmd" "staging reload"; fi
}

redeploy_api() {
    echo "== Force new deployment of staging API service =="
    run aws ecs update-service --cluster "$API_CLUSTER" --service "$API_SERVICE" \
        --force-new-deployment --region "$REGION" --no-cli-pager
}

main() {
    assert_staging_only
    echo "Target: STAGING  (jobs=$JOBS_STACK, prefix=$S3_PREFIX, tag=$IMAGE_TAG)"
    rebuild_kg
    login_ecr
    build_and_push_images
    sync_kg_to_staging
    run_staging_etl
    redeploy_api
    echo "Done. Staging RDS reloaded and API redeployed. Verify the /bioactivity endpoints."
}

main

#!/usr/bin/env bash
# Monthly IE → KGC → DB pipeline orchestrator.
#
# Usage:
#   bash infra/monthly_pipeline.sh [OPTIONS]
#
# Options:
#   --date YYYY_MM_DD   Override run date (default: today UTC)
#   --skip-ie           Skip IE pipeline, use existing extraction outputs
#   --skip-ingest       Skip KGC ingest stage (ontologies rarely change)
#   --skip-db           Skip database load step
#   --skip-s3           Skip S3 upload of KGC parquet outputs
#   --ie-only           Run IE pipeline only, stop before KGC
#
# Environment variables (see monthly_pipeline.env.example):
#   OPENAI_API_KEY              Required for IE extraction and KGC enrichment
#   NCBI_API_KEY                Required for IE corpus/search stages
#   NCBI_EMAIL                  Required for IE corpus/search stages
#   FOODATLAS_PARQUET_BUCKET    S3 bucket for KGC parquet outputs (required
#                               unless --skip-s3 is set). The AWS CLI must
#                               be available and authenticated.
#   FOODATLAS_PARQUET_PREFIX    Optional key prefix within the bucket
#                               (default: kg)

set -euo pipefail

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
IE_DIR="$REPO_ROOT/backend/ie"
KGC_DIR="$REPO_ROOT/backend/kgc"
DB_DIR="$REPO_ROOT/backend/db"
LOCK_FILE="$SCRIPT_DIR/.pipeline.lock"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
RUN_DATE="$(date -u +%Y_%m_%d)"
SKIP_IE=false
SKIP_INGEST=false
SKIP_DB=false
SKIP_S3=false
IE_ONLY=false

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --date)       RUN_DATE="$2"; shift 2 ;;
        --skip-ie)    SKIP_IE=true;  shift ;;
        --skip-ingest) SKIP_INGEST=true; shift ;;
        --skip-db)    SKIP_DB=true;  shift ;;
        --skip-s3)    SKIP_S3=true;  shift ;;
        --ie-only)    IE_ONLY=true;  shift ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR="$SCRIPT_DIR/logs/$RUN_DATE"
mkdir -p "$LOG_DIR"

log() { echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_DIR/pipeline.log"; }

# ---------------------------------------------------------------------------
# Lock file — prevent concurrent runs
# ---------------------------------------------------------------------------
cleanup() {
    rm -f "$LOCK_FILE"
    log "Lock released."
}

if [[ -f "$LOCK_FILE" ]]; then
    echo "ERROR: Pipeline already running (lock file exists: $LOCK_FILE)" >&2
    echo "If a previous run crashed, remove the lock file manually." >&2
    exit 1
fi
echo "$$" > "$LOCK_FILE"
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Environment validation
# ---------------------------------------------------------------------------
validate_env() {
    local missing=()

    if [[ -z "${OPENAI_API_KEY:-}" ]]; then
        missing+=("OPENAI_API_KEY")
    fi

    if [[ "$SKIP_IE" == false ]]; then
        [[ -z "${NCBI_API_KEY:-}" ]] && missing+=("NCBI_API_KEY")
        [[ -z "${NCBI_EMAIL:-}" ]]   && missing+=("NCBI_EMAIL")
    fi

    if ! command -v uv &>/dev/null; then
        echo "ERROR: uv not found on PATH." >&2
        exit 1
    fi

    if [[ "$SKIP_S3" == false ]]; then
        [[ -z "${FOODATLAS_PARQUET_BUCKET:-}" ]] && missing+=("FOODATLAS_PARQUET_BUCKET")
        if ! command -v aws &>/dev/null; then
            echo "ERROR: aws CLI not found on PATH (required unless --skip-s3)." >&2
            exit 1
        fi
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "ERROR: Missing required environment variables: ${missing[*]}" >&2
        echo "See infra/monthly_pipeline.env.example for details." >&2
        exit 1
    fi
}

# Source .env files if they exist (won't overwrite existing vars).
[[ -f "$REPO_ROOT/.env" ]] && set -a && source "$REPO_ROOT/.env" && set +a
[[ -f "$IE_DIR/.env" ]]    && set -a && source "$IE_DIR/.env" && set +a
[[ -f "$KGC_DIR/.env" ]]   && set -a && source "$KGC_DIR/.env" && set +a

validate_env

# ---------------------------------------------------------------------------
# Stage 1: IE Pipeline
# ---------------------------------------------------------------------------
run_ie() {
    log "=== IE Pipeline (date=$RUN_DATE) ==="
    cd "$IE_DIR"

    local ie_args=("run")
    if [[ "$RUN_DATE" != "$(date -u +%Y_%m_%d)" ]]; then
        ie_args+=("--date" "$RUN_DATE")
    fi

    log "Running: uv run python main.py ${ie_args[*]}"
    uv run python main.py "${ie_args[@]}" 2>&1 | tee "$LOG_DIR/ie.log"
    log "IE pipeline complete."
}

# ---------------------------------------------------------------------------
# Stage 2: KGC Pipeline
# ---------------------------------------------------------------------------
run_kgc() {
    log "=== KGC Pipeline ==="
    cd "$KGC_DIR"

    local kgc_args=("run")
    if [[ "$SKIP_INGEST" == true ]]; then
        kgc_args+=("--stages" "1:4")
    fi

    export KGC_IE_RAW_DIR="$IE_DIR/outputs/extraction"
    log "KGC_IE_RAW_DIR=$KGC_IE_RAW_DIR"
    log "Running: uv run python main.py ${kgc_args[*]}"
    uv run python main.py "${kgc_args[@]}" 2>&1 | tee "$LOG_DIR/kgc.log"
    log "KGC pipeline complete."
}

# ---------------------------------------------------------------------------
# Stage 3: Database Load
# ---------------------------------------------------------------------------
run_db() {
    log "=== DB Load ==="
    cd "$DB_DIR"

    log "Running: uv run python main.py load --parquet-dir ../kgc/outputs/kg"
    uv run python main.py load --parquet-dir ../kgc/outputs/kg 2>&1 \
        | tee "$LOG_DIR/db.log"
    log "DB load complete."
}

# ---------------------------------------------------------------------------
# Stage 4: S3 Upload (KGC parquet outputs)
# ---------------------------------------------------------------------------
run_s3_upload() {
    log "=== S3 Upload ==="
    local prefix="${FOODATLAS_PARQUET_PREFIX:-kg}"
    local source_dir="$KGC_DIR/outputs/kg"
    local dest="s3://${FOODATLAS_PARQUET_BUCKET}/${prefix}"

    if [[ ! -d "$source_dir" ]]; then
        log "Skipping S3 upload: source directory missing ($source_dir)."
        return
    fi

    log "Running: aws s3 sync $source_dir $dest"
    aws s3 sync "$source_dir" "$dest" --delete 2>&1 | tee "$LOG_DIR/s3.log"
    log "S3 upload complete."
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
log "Pipeline started (date=$RUN_DATE, skip_ie=$SKIP_IE, skip_ingest=$SKIP_INGEST, skip_db=$SKIP_DB, skip_s3=$SKIP_S3, ie_only=$IE_ONLY)"

if [[ "$SKIP_IE" == false ]]; then
    run_ie
fi

if [[ "$IE_ONLY" == true ]]; then
    log "IE-only mode — stopping before KGC."
    log "Pipeline finished."
    exit 0
fi

run_kgc

if [[ "$SKIP_DB" == false ]]; then
    run_db
fi

if [[ "$SKIP_S3" == false ]]; then
    run_s3_upload
fi

log "Pipeline finished successfully."

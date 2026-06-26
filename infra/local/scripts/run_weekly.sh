#!/usr/bin/env bash
# Weekly FoodAtlas refresh: IE (local Gemma 4 via SLURM) -> KGC -> newsletter.
#
# Usage:
#   bash infra/local/scripts/run_weekly.sh [OPTIONS]
#
# Options:
#   --date YYYY_MM_DD   Override run date (default: today UTC)
#   --version vX.Y      Override the auto-incremented release version (the
#                       next version is derived from release_notes/ on publish)
#   --skip-ingest       Skip KGC ingest stage (ontologies rarely change)
#   --ie-only           Run IE pipeline only, stop before KGC
#   --build-and-publish Run the full pipeline, then push to the LIVE site
#                       (S3 + RDS + public bundle). Off by default.
#   --publish           Publish existing outputs/ to the LIVE site without
#                       rebuilding. Pair with --version vX.Y for the bundle.
#   --dry-run           Preview the publish (sync --dryrun, no RDS task, bundle
#                       built locally) — nothing is uploaded or loaded.
#   --no-prompt         Non-interactive: skip checkpoints / auto-confirm publish
#
# Required environment variables:
#   NCBI_EMAIL, NCBI_API_KEY     IE search (NCBI E-utilities)
#   GOOGLE_API_KEY               KGC trust stage (Gemini)
#   ANTHROPIC_API_KEY            KGC evaluation + newsletter (Claude)
#   AWS_REGION + credentials     Only with --publish
#
# Unlike run_monthly.sh (OpenAI batch + local DB), IE stage 4 runs the local
# open-source Gemma 4 model on 4x GPU via SLURM, and --publish targets the live
# production RDS. The Gemma job can take hours; run under nohup/tmux to survive
# SSH drops.

set -euo pipefail

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
IE_DIR="$REPO_ROOT/backend/ie"
KGC_DIR="$REPO_ROOT/backend/kgc"
AWS_DIR="$REPO_ROOT/infra/aws"
LOCK_FILE="$SCRIPT_DIR/.pipeline-weekly.lock"

# Local Gemma inference
GEMMA_ENV="/mnt/share/kaichixie/miniconda3/envs/gemma4_infer"
GEMMA_MODEL_DIR="/mnt/share/kaichixie/model_weights/gemma-4-31B-it"
GEMMA_MODEL_NAME="gemma-4-31B-it"
GEMMA_PARTITION="aifshpc1"
GEMMA_GPUS=4

# SLURM can't see GPUs used outside SLURM, so it would schedule the job onto
# busy GPUs and vLLM would crash. Gate on ACTUAL free GPU memory before
# submitting: a GPU with < GPU_FREE_MIB used counts as free.
GPU_FREE_MIB=2000
GPU_WAIT_INTERVAL=120     # seconds between checks
GPU_WAIT_TIMEOUT=43200    # give up after 12h

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
RUN_DATE="$(date -u +%Y_%m_%d)"
SKIP_INGEST=false
IE_ONLY=false
PUBLISH=false
PUBLISH_ONLY=false    # skip the build, publish existing outputs/
DRY_RUN=""            # preview the publish without mutating S3/RDS
INTERACTIVE=true
RELEASE_VERSION=""    # auto-incremented for publishes (v4.2 -> v4.3); --version overrides

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --date)        RUN_DATE="$2"; shift 2 ;;
        --skip-ingest) SKIP_INGEST=true; shift ;;
        --ie-only)     IE_ONLY=true; shift ;;
        --build-and-publish) PUBLISH=true; shift ;;
        --publish)           PUBLISH=true; PUBLISH_ONLY=true; shift ;;
        --no-prompt)         INTERACTIVE=false; shift ;;
        --dry-run)           DRY_RUN=1; shift ;;
        --version)           RELEASE_VERSION="$2"; shift 2 ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR="$SCRIPT_DIR/logs/weekly/$RUN_DATE"
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
    echo "ERROR: Pipeline already running (lock file: $LOCK_FILE)." >&2
    echo "If a previous run crashed, remove the lock file manually." >&2
    exit 1
fi
echo "$$" > "$LOCK_FILE"
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Environment — source .env files, bridge the Gemini key, pin the IE date
# ---------------------------------------------------------------------------
[[ -f "$REPO_ROOT/.env" ]] && set -a && source "$REPO_ROOT/.env" && set +a
[[ -f "$IE_DIR/.env" ]]    && set -a && source "$IE_DIR/.env"    && set +a
[[ -f "$KGC_DIR/.env" ]]   && set -a && source "$KGC_DIR/.env"   && set +a

export GOOGLE_API_KEY="${GOOGLE_API_KEY:-${GEMINI_API_KEY:-}}"  # trust authenticates via GOOGLE_API_KEY
export IE_DATE="$RUN_DATE"

# ---------------------------------------------------------------------------
# Environment validation
# ---------------------------------------------------------------------------
validate_env() {
    local missing=()

    command -v uv &>/dev/null || { echo "ERROR: uv not found on PATH." >&2; exit 1; }

    if [[ "$PUBLISH_ONLY" == false ]]; then
        command -v sbatch &>/dev/null || { echo "ERROR: sbatch not found on PATH." >&2; exit 1; }
        [[ -x "$GEMMA_ENV/bin/python" ]] || { echo "ERROR: Gemma env missing: $GEMMA_ENV" >&2; exit 1; }
        [[ -d "$GEMMA_MODEL_DIR" ]]      || { echo "ERROR: Gemma weights missing: $GEMMA_MODEL_DIR" >&2; exit 1; }
        [[ -z "${NCBI_EMAIL:-}" ]] && missing+=("NCBI_EMAIL")
        if [[ "$IE_ONLY" == false ]]; then
            [[ -z "${GOOGLE_API_KEY:-}" ]]    && missing+=("GOOGLE_API_KEY (KGC trust)")
            [[ -z "${ANTHROPIC_API_KEY:-}" ]] && missing+=("ANTHROPIC_API_KEY (KGC eval + newsletter)")
        fi
        [[ -z "${NCBI_API_KEY:-}" ]] && log "WARN: NCBI_API_KEY unset — PubMed search ~3x slower."
    fi
    if [[ "$PUBLISH" == true ]]; then
        command -v aws &>/dev/null || { echo "ERROR: aws CLI not found (required for --publish)." >&2; exit 1; }
        [[ -z "${AWS_REGION:-}" ]] && missing+=("AWS_REGION")
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "ERROR: Missing required environment: ${missing[*]}" >&2
        exit 1
    fi
    return 0
}

# Pause between stages unless --no-prompt.
checkpoint() {
    [[ "$INTERACTIVE" == false ]] && return 0
    read -rp "Continue past $1? [y/N] " response
    [[ "$response" =~ ^[Yy]$ ]] || { log "Aborted by user after $1."; exit 0; }
}

# ---------------------------------------------------------------------------
# Stage 1: IE search -> retrieval -> filter (uv)
# ---------------------------------------------------------------------------
run_ie() {
    local filtered="$IE_DIR/outputs/filtering/$RUN_DATE/filtered_sentences/information_extraction_input.tsv"
    if [[ -f "$filtered" ]]; then
        log "=== IE: search/filter already complete — skipping ==="
        return 0
    fi
    log "=== IE: search -> retrieval -> filter (date=$RUN_DATE) ==="
    cd "$IE_DIR"
    uv run python main.py run --stages 1:3 2>&1 | tee "$LOG_DIR/ie.log"
    log "IE search/filter complete."
}

# ---------------------------------------------------------------------------
# Stage 2: IE extraction — local Gemma 4 via SLURM (blocks until done)
# ---------------------------------------------------------------------------
run_gemma() {
    local out_dir="$IE_DIR/outputs/extraction/$RUN_DATE"
    local input="$IE_DIR/outputs/filtering/$RUN_DATE/filtered_sentences/information_extraction_input.tsv"
    local sbatch="$LOG_DIR/gemma.sbatch"

    if [[ -f "$out_dir/extraction_predicted.json" ]]; then
        log "=== IE: Gemma extraction already done — skipping ==="
        return 0
    fi
    log "=== IE: Gemma 4 extraction via SLURM (gpus=$GEMMA_GPUS) ==="
    [[ -f "$input" ]] || { echo "ERROR: Gemma input missing: $input" >&2; exit 1; }
    mkdir -p "$out_dir"

    wait_for_gpus "$GEMMA_GPUS"
    write_gemma_sbatch "$sbatch" "$out_dir" "$input"
    submit_and_wait "$sbatch"
    convert_predictions "$out_dir"
    log "Gemma extraction complete."
}

# Block until `need` GPUs are actually free, polling nvidia-smi (SLURM is blind
# to GPUs used outside SLURM, so submitting blindly would crash on busy GPUs).
wait_for_gpus() {
    local need="$1" waited=0 free
    while :; do
        free=$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits 2>/dev/null \
            | awk -v t="$GPU_FREE_MIB" '$1 < t { n++ } END { print n + 0 }')
        if [[ "${free:-0}" -ge "$need" ]]; then
            log "  GPUs free: $free/$need — submitting."
            return 0
        fi
        if [[ "$waited" -ge "$GPU_WAIT_TIMEOUT" ]]; then
            echo "ERROR: timed out (${GPU_WAIT_TIMEOUT}s) waiting for $need free GPUs (only ${free:-0} free)." >&2
            exit 1
        fi
        log "  Waiting for GPUs: ${free:-0}/$need free (others in use outside SLURM); retry in ${GPU_WAIT_INTERVAL}s..."
        sleep "$GPU_WAIT_INTERVAL"
        waited=$((waited + GPU_WAIT_INTERVAL))
    done
}

# Generate the date-parameterized sbatch file.
write_gemma_sbatch() {
    local sbatch="$1" out_dir="$2" input="$3" devices
    devices="$(seq -s, 0 $((GEMMA_GPUS - 1)))"
    cat > "$sbatch" <<SBATCH
#!/usr/bin/env bash
#SBATCH --job-name=gemma4-ie-$RUN_DATE
#SBATCH --partition=$GEMMA_PARTITION
#SBATCH --nodes=1
#SBATCH --gres=gpu:$GEMMA_GPUS
#SBATCH --cpus-per-task=16
#SBATCH --mem=180G
#SBATCH --time=12:00:00
#SBATCH --output=$LOG_DIR/gemma_%j.out
#SBATCH --error=$LOG_DIR/gemma_%j.err
set -eo pipefail
cd "$IE_DIR"
export PATH="$GEMMA_ENV/bin:\$PATH"
export CUDA_VISIBLE_DEVICES=$devices
export TOKENIZERS_PARALLELISM=false VLLM_LOGGING_LEVEL=WARNING
"$GEMMA_ENV/bin/python" -m src.pipeline.extraction.gemma.runner \\
    --input_path  $input \\
    --output_dir  $out_dir \\
    --model_dir   $GEMMA_MODEL_DIR \\
    --tensor_parallel_size $GEMMA_GPUS \\
    --gpu_memory_utilization 0.85
SBATCH
}

# Submit and block until the job ends; surface its log on failure.
submit_and_wait() {
    local sbatch="$1" out rc job
    log "  Submitting $sbatch (blocking until complete)..."
    set +e; out="$(sbatch --wait --parsable "$sbatch")"; rc=$?; set -e
    job="${out%%;*}"
    if [[ "$rc" -ne 0 ]]; then
        log "ERROR: Gemma SLURM job ${job:-?} failed (exit $rc):"
        tail -n 25 "$LOG_DIR/gemma_${job}.err" 2>/dev/null | sed 's/^/    /'
        exit 1
    fi
    log "  SLURM job $job finished."
}

# Convert the Gemma TSV to the JSON KGC reads + record model provenance.
convert_predictions() {
    local out_dir="$1" tsv="$1/extraction_predicted.tsv"
    [[ -f "$tsv" ]] || { echo "ERROR: Gemma produced no $tsv" >&2; exit 1; }
    cd "$IE_DIR"
    uv run python -m src.pipeline.extraction.parse_predictions --input_path "$tsv" \
        2>&1 | tee -a "$LOG_DIR/ie.log"
    printf '{\n  "model": "%s",\n  "date": "%s"\n}\n' "$GEMMA_MODEL_NAME" "$RUN_DATE" \
        > "$out_dir/run_info.json"
}

# ---------------------------------------------------------------------------
# Stage 3: KGC pipeline (ingest -> ... -> evaluation -> newsletter)
# ---------------------------------------------------------------------------
run_kgc() {
    log "=== KGC: ingest -> ... -> evaluation -> newsletter ==="
    cd "$KGC_DIR"
    export KGC_IE_RAW_DIR="$IE_DIR/outputs/extraction"

    local args=("run")
    [[ "$SKIP_INGEST" == true ]] && args+=("--stages" "1:7")
    uv run python main.py "${args[@]}" 2>&1 | tee "$LOG_DIR/kgc.log"
    log "KGC complete — wrote outputs/kg/newsletter.json"
}

# ---------------------------------------------------------------------------
# Stage 4: render the weekly newsletter HTML from newsletter.json + template
# ---------------------------------------------------------------------------
run_newsletter() {
    log "=== Render newsletter HTML ==="
    cd "$KGC_DIR"
    uv run python scripts/render_newsletter.py 2>&1 | tee "$LOG_DIR/newsletter.log"
    log "Newsletter: $KGC_DIR/outputs/newsletter.html"

    if [[ -n "$RELEASE_VERSION" ]]; then
        uv run python scripts/render_summary.py --version "$RELEASE_VERSION" 2>&1 \
            | tee -a "$LOG_DIR/newsletter.log"
        log "Summary: $KGC_DIR/release_notes/SUMMARY-$RELEASE_VERSION.md"
    else
        log "Summary: skipped (pass --version vX.Y to emit SUMMARY-vX.Y.md)"
    fi
}

# ---------------------------------------------------------------------------
# Stage 5 (--publish only): release report + publish to S3 + load production RDS
# ---------------------------------------------------------------------------
run_publish() {
    local tag="LIVE"; [[ -n "$DRY_RUN" ]] && tag="DRY-RUN"
    [[ "$INTERACTIVE" == true && -z "$DRY_RUN" ]] && confirm_publish
    write_changelog

    log "=== $tag: publish KGC outputs to S3 ==="
    cd "$KGC_DIR"
    ./scripts/sync-outputs-to-s3.sh ${DRY_RUN:+--dry-run} 2>&1 | tee "$LOG_DIR/s3.log"

    log "=== $tag: load production RDS (Fargate ETL) ==="
    cd "$AWS_DIR"
    ./scripts/run-data-load.sh ${DRY_RUN:+--dry-run} 2>&1 | tee "$LOG_DIR/rds.log"
    [[ -z "$DRY_RUN" ]] && log "Production RDS load complete — live website now serves the new data."

    if [[ -n "$RELEASE_VERSION" ]]; then
        log "=== $tag: publish public release bundle ($RELEASE_VERSION) ==="
        cd "$KGC_DIR"
        ./scripts/publish-bundle.sh "$RELEASE_VERSION" "release_notes/SUMMARY-$RELEASE_VERSION.md" ${DRY_RUN:+--dry-run} \
            2>&1 | tee "$LOG_DIR/bundle.log"
        [[ -z "$DRY_RUN" ]] && log "Public bundle published: foodatlas-$RELEASE_VERSION.zip (downloads bucket + index.json)."
    else
        log "Public bundle: skipped (pass --version vX.Y to publish a user-facing release)."
    fi
    if [[ -n "$DRY_RUN" ]]; then
        log "DRY-RUN complete — nothing was uploaded or loaded."
    fi
}

confirm_publish() {
    log "About to publish to the LIVE site: S3 sync + production RDS rebuild (mv_* tables)${RELEASE_VERSION:+ + public bundle $RELEASE_VERSION}."
    read -rp "Proceed with the PRODUCTION update? [y/N] " response
    [[ "$response" =~ ^[Yy]$ ]] || { log "Publish declined by user."; exit 0; }
}

# CHANGELOG.md is required by the S3 publish step; the report command produces
# it by diffing against data/PreviousFAKG. Write a placeholder if there's no
# baseline (loudly warned, not silently faked).
write_changelog() {
    log "=== KGC release report (CHANGELOG.md) ==="
    cd "$KGC_DIR"
    if ! uv run python main.py report 2>&1 | tee "$LOG_DIR/report.log"; then
        log "  WARN: report failed (no data/PreviousFAKG baseline?) — writing placeholder."
        printf '# FoodAtlas KG — %s\n\nDiff report unavailable on this host.\n' "$RUN_DATE" \
            > "$KGC_DIR/outputs/kg/CHANGELOG.md"
    fi
}

# Derive the next release version by bumping the highest SUMMARY-vX.Y.md in
# release_notes/. Each weekly publish increments MINOR (v4.2 -> v4.3 -> ...);
# pass --version explicitly to override (e.g. a major bump).
next_version() {
    local latest major minor
    latest=$(ls "$KGC_DIR"/release_notes/SUMMARY-v*.md 2>/dev/null \
        | sed -E 's|.*/SUMMARY-v([0-9]+\.[0-9]+)\.md|\1|' \
        | sort -t. -k1,1n -k2,2n | tail -1) || true
    [[ -z "$latest" ]] && { echo "v4.3"; return; }
    major="${latest%%.*}"; minor="${latest##*.}"
    echo "v${major}.$((minor + 1))"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
log "Weekly pipeline started (date=$RUN_DATE, skip_ingest=$SKIP_INGEST, ie_only=$IE_ONLY, publish=$PUBLISH)"

validate_env

# Auto-increment the release version for publishes unless one was given.
if [[ -z "$RELEASE_VERSION" && "$PUBLISH" == true && "$PUBLISH_ONLY" == false ]]; then
    RELEASE_VERSION="$(next_version)"
    log "Auto-versioned release: $RELEASE_VERSION"
fi

if [[ "$PUBLISH_ONLY" == true ]]; then
    log "Publishing existing outputs/ — skipping build (--publish)."
else
    run_ie
    run_gemma

    if [[ "$IE_ONLY" == true ]]; then
        log "IE-only mode — stopping before KGC. Extractions: $IE_DIR/outputs/extraction/$RUN_DATE/"
        exit 0
    fi

    checkpoint "IE"
    run_kgc
    run_newsletter
fi

if [[ "$PUBLISH" == false ]]; then
    log "Weekly run finished (local outputs only). Re-run with --publish to push to the live site."
    exit 0
fi

checkpoint "KGC + newsletter"
run_publish
[[ -n "$DRY_RUN" ]] && log "Weekly pipeline finished (dry-run — no live changes)." \
                    || log "Weekly pipeline finished successfully (published to live)."

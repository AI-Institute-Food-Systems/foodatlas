#!/bin/bash
# =============================================================================
# FoodAtlas Pipeline Orchestrator
# Submits all pipeline steps as SLURM jobs in sequence using afterok deps.
#
# Usage:
#   bash scripts/run_pipeline.sh [DATE] [MODEL_NAME]
#
# Arguments:
#   DATE        Run date tag used for outputs/text_parser/{DATE}/ subfolder
#               and parse step filenames (default: today, format YYYY_MM_DD)
#   MODEL_NAME  Model name for parse step  (default: gpt-5.2)
#
# Environment variables:
#   OPENAI_API_KEY      Required for step 5 (information extraction)
#   NCBI_API_KEY        Optional API key for NCBI E-utilities
#   NCBI_EMAIL          Email for NCBI E-utility API (default: user@example.com)
#   SLURM_MAIL_USER     Email for SLURM notifications (optional)
#   CONDA_ENV_NAME      Conda environment name (default: foodatlas_pipeline)
#   CONDA_SH_PATH       Path to conda.sh (default: from CONDA_EXE or conda)
#
# Example:
#   bash scripts/run_pipeline.sh 2026_03_22 gpt-5.2
# =============================================================================

set -euo pipefail

PIPELINE_DIR=$(cd "$(dirname "$0")/.." && pwd)
LOG_DIR=${PIPELINE_DIR}/scripts/logs
DATE=${1:-$(date +%Y_%m_%d)}
MODEL_NAME=${2:-gpt-5.2}
RUN_DIR=${PIPELINE_DIR}/outputs/text_parser/${DATE}
CONDA_ENV=${CONDA_ENV_NAME:-foodatlas_pipeline}
MAIL_USER=${SLURM_MAIL_USER:-}

# Locate conda.sh from environment or default
if [[ -n "${CONDA_SH_PATH:-}" ]]; then
    source "${CONDA_SH_PATH}"
elif [[ -n "${CONDA_EXE:-}" ]]; then
    source "$(dirname "$(dirname "${CONDA_EXE}")")/etc/profile.d/conda.sh"
else
    echo "ERROR: Set CONDA_SH_PATH or ensure conda is on PATH" >&2
    exit 1
fi
conda activate "${CONDA_ENV}"
PYTHON=$(which python)

mkdir -p "${LOG_DIR}"
mkdir -p "${RUN_DIR}/retrieved_sentences"
mkdir -p "${RUN_DIR}/sentence_filtering"
mkdir -p "${RUN_DIR}/filtered_sentences"

echo "FoodAtlas pipeline | date=${DATE} model=${MODEL_NAME} | $(date)"

# Build mail flags only when MAIL_USER is set
MAIL_FLAGS=()
if [[ -n "${MAIL_USER}" ]]; then
    MAIL_FLAGS=(--mail-type=ALL "--mail-user=${MAIL_USER}")
fi

# Submit a job, optionally depending on a prior job ID.
sbatch_after() {
    local prev=${1:-}; shift
    local dep_flag=""
    [[ -n "$prev" ]] && dep_flag="--dependency=afterok:${prev}"
    sbatch --parsable $dep_flag "${MAIL_FLAGS[@]}" "$@"
}
LAST_JOB=""

# ---------------------------------------------------------------------------
# Step 0: Download PMC ID map
# ---------------------------------------------------------------------------
JOB0=$(sbatch --parsable \
    --job-name=download_PMC_id_map \
    "${MAIL_FLAGS[@]}" \
    --output="${LOG_DIR}/${DATE}_step0_download_%j.out" \
    --error="${LOG_DIR}/${DATE}_step0_download_%j.err" \
    --nodes=1 \
    --ntasks=1 \
    --cpus-per-task=2 \
    --mem=4G \
    --time=02:00:00 \
    --wrap="set -eu
        cd ${PIPELINE_DIR}
        rm -rf data/NCBI && mkdir -p data/NCBI
        wget https://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz -P data/NCBI/
        gunzip -f data/NCBI/PMC-ids.csv.gz
    ")
echo "  [0] download_PMC_id_map ${JOB0}"
LAST_JOB=$JOB0

# ---------------------------------------------------------------------------
# Step 1: Update BioC-PMC
# ---------------------------------------------------------------------------
JOB1=$(sbatch_after "$LAST_JOB" \
    --job-name=BioC-PMC-update \
    --output="${LOG_DIR}/${DATE}_step1_bioc_update_%j.out" \
    --error="${LOG_DIR}/${DATE}_step1_bioc_update_%j.err" \
    --nodes=1 \
    --ntasks=1 \
    --cpus-per-task=4 \
    --mem=8G \
    --time=48:00:00 \
    --wrap="
        set -eu
        cd ${PIPELINE_DIR}
        ${PYTHON} -u src/lit2kg/0_update_PMC_BioC.py
    ")
LAST_JOB=$JOB1
echo "  [1] update_PMC_BioC     ${JOB1}"

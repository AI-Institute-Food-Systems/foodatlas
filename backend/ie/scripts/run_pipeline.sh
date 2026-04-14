#!/bin/bash
# =============================================================================
# FoodAtlas Pipeline Orchestrator
# Submits all pipeline steps as SLURM jobs in sequence using afterok deps.
#
# Usage:
#   bash scripts/run_pipeline.sh [DATE] [MODEL_NAME]
#
# Arguments:
#   DATE        Run date tag used for outputs/search/{DATE}/ subfolder
#               and parse step filenames (default: today, format YYYY_MM_DD)
#   MODEL_NAME  Model name for parse step  (default: gpt-5.2)
#
# Environment variables:
#   OPENAI_API_KEY      Required for step 5 (information extraction)
#   NCBI_API_KEY        Optional API key for NCBI E-utilities
#   NCBI_EMAIL          Email for NCBI E-utility API (default: user@example.com)
#   SLURM_MAIL_USER     Email for SLURM notifications (optional)
#
# Example:
#   bash scripts/run_pipeline.sh 2026_03_22 gpt-5.2
# =============================================================================

set -euo pipefail

PIPELINE_DIR=$(cd "$(dirname "$0")/.." && pwd)
LOG_DIR=${PIPELINE_DIR}/scripts/logs
DATE=${1:-$(date +%Y_%m_%d)}
MODEL_NAME=${2:-gpt-5.2}
RUN_DIR=${PIPELINE_DIR}/outputs/search/${DATE}
MAIL_USER=${SLURM_MAIL_USER:-}
UV_RUN="uv run --project ${PIPELINE_DIR}"

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
        ${UV_RUN} python -u -m src.pipeline.corpus.update_bioc
    ")
LAST_JOB=$JOB1
echo "  [1] update_PMC_BioC     ${JOB1}"

# ---------------------------------------------------------------------------
# Step 2: Search PubMed / PMC
# ---------------------------------------------------------------------------
# JOB2=$(sbatch_after "$LAST_JOB" \
#     --job-name=search_pubmed_pmc \
#     "${MAIL_FLAGS[@]}" \
#     --output="${LOG_DIR}/${DATE}_step2_search_%j.out" \
#     --error="${LOG_DIR}/${DATE}_step2_search_%j.err" \
#     --nodes=1 \
#     --ntasks=1 \
#     --cpus-per-task=8 \
#     --mem=8G \
#     --time=72:00:00 \
#     --wrap="
#         set -eu
#         cd ${PIPELINE_DIR}
#         ${UV_RUN} python -u src/lit2kg/1_search_pubmed_pmc.py \
#             --query data/food_terms.txt \
#             --query_uid_results_filepath outputs/search/${DATE}/query_uid_results.tsv \
#             --filtered_sentences_filepath outputs/search/${DATE}/retrieved_sentences/result_{i}.tsv \
#             --filepath_BioC_PMC \${BIOC_PMC_DIR:-/mnt/data/shared/BioC-PMC}
#     ")
# LAST_JOB=$JOB2
# echo "  [2] search_pubmed_pmc   ${JOB2}"

# ---------------------------------------------------------------------------
# Step 3: BioBERT inference  (GPU required)
# ---------------------------------------------------------------------------
# JOB3=$(sbatch_after "$LAST_JOB" \
#     --job-name=biobert_inference \
#     "${MAIL_FLAGS[@]}" \
#     --output="${LOG_DIR}/${DATE}_step3_biobert_%j.out" \
#     --error="${LOG_DIR}/${DATE}_step3_biobert_%j.err" \
#     --nodes=1 \
#     --ntasks=1 \
#     --cpus-per-task=8 \
#     --gres=gpu:1 \
#     --mem=32G \
#     --time=48:00:00 \
#     --wrap="
#         set -eu
#         cd ${PIPELINE_DIR}
#         ${UV_RUN} python -u src/lit2kg/2_run_sentence_filtering.py \
#             --input_file_path outputs/search/${DATE}/retrieved_sentences/sentence_filtering_input.tsv \
#             --save_file_path  outputs/search/${DATE}/sentence_filtering \
#             --model_dir       outputs/biobert_binary_prod \
#             --sentence_col    sentence \
#             --chunk_size      10000 \
#             --batch_size      64
#     ")
# LAST_JOB=$JOB3
# echo "  [3] biobert_inference   ${JOB3}"

# ---------------------------------------------------------------------------
# Step 4: Aggregate sentence filtering results & deduplicate against past runs
# ---------------------------------------------------------------------------
# JOB4=$(sbatch_after "$LAST_JOB" \
#     --job-name=aggregate_sentence_filtering \
#     "${MAIL_FLAGS[@]}" \
#     --output="${LOG_DIR}/${DATE}_step4_aggregate_%j.out" \
#     --error="${LOG_DIR}/${DATE}_step4_aggregate_%j.err" \
#     --nodes=1 \
#     --ntasks=1 \
#     --cpus-per-task=2 \
#     --mem=8G \
#     --time=02:00:00 \
#     --wrap="
#         set -eu
#         cd ${PIPELINE_DIR}
#         ${UV_RUN} python -u src/lit2kg/3_aggregate_sentence_filtering_results.py \
#             --input_dir       outputs/search/${DATE}/sentence_filtering \
#             --aggregated_path outputs/search/${DATE}/filtered_sentences/filtered_sentence_aggregated.tsv \
#             --ie_input_path   outputs/search/${DATE}/filtered_sentences/information_extraction_input.tsv \
#             --reference_dir   outputs/extraction \
#             --threshold       0.99
#     ")
# LAST_JOB=$JOB4
# echo "  [4] aggregate_filtering ${JOB4}"

# ---------------------------------------------------------------------------
# Step 5: Information extraction
# ---------------------------------------------------------------------------
# JOB5=$(sbatch_after "$LAST_JOB" \
#     --job-name=information_extraction \
#     "${MAIL_FLAGS[@]}" \
#     --output="${LOG_DIR}/${DATE}_step5_ie_%j.out" \
#     --error="${LOG_DIR}/${DATE}_step5_ie_%j.err" \
#     --nodes=1 \
#     --ntasks=1 \
#     --cpus-per-task=4 \
#     --mem=16G \
#     --time=48:00:00 \
#     --wrap="
#         set -euo pipefail
#         cd ${PIPELINE_DIR}
#         echo 'Starting information extraction: \$(date)'
#         ${UV_RUN} python -u src/lit2kg/4_run_information_extraction.py \
#             --input_path  outputs/search/${DATE}/filtered_sentences/information_extraction_input.tsv \
#             --output_dir  outputs/extraction/${DATE}_prediction_batch \
#             --model       ${MODEL_NAME} \
#             --date        ${DATE}
#         echo 'Finished: \$(date)'
#     ")
# LAST_JOB=$JOB5
# echo "  [5] information_extraction ${JOB5}"

# ---------------------------------------------------------------------------
# Step 6: Parse text parser predictions
# ---------------------------------------------------------------------------
# JOB6=$(sbatch_after "$LAST_JOB" \
#     --job-name=parse_predictions \
#     "${MAIL_FLAGS[@]}" \
#     --output="${LOG_DIR}/${DATE}_step6_parse_%j.out" \
#     --error="${LOG_DIR}/${DATE}_step6_parse_%j.err" \
#     --nodes=1 \
#     --ntasks=1 \
#     --cpus-per-task=2 \
#     --mem=8G \
#     --time=04:00:00 \
#     --wrap="
#         set -euo pipefail
#         cd ${PIPELINE_DIR}
#         echo 'Parsing predictions: \$(date)'
#         ${UV_RUN} python -u src/lit2kg/5_parse_text_parser_predictions.py \
#             --batch_input_path outputs/extraction/${DATE}_prediction_batch/batch_input_${DATE}.tsv \
#             --batch_results_dir outputs/extraction/${DATE}_prediction_batch \
#             --output_tsv outputs/extraction/extraction_predicted_${DATE}.tsv \
#             --model_name ${MODEL_NAME}
#         echo 'Done: \$(date)'
#     ")
# LAST_JOB=$JOB6
# echo "  [6] parse_predictions   ${JOB6}"

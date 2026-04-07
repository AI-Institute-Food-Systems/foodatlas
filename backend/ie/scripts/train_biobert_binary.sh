#!/bin/bash
#SBATCH --job-name=biobert_binary
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gres=gpu:1
#SBATCH --mem=32G
#SBATCH --time=12:00:00

set -euo pipefail

# Locate conda.sh from environment or default
if [[ -n "${CONDA_SH_PATH:-}" ]]; then
    source "${CONDA_SH_PATH}"
elif [[ -n "${CONDA_EXE:-}" ]]; then
    source "$(dirname "$(dirname "${CONDA_EXE}")")/etc/profile.d/conda.sh"
else
    echo "ERROR: Set CONDA_SH_PATH or ensure conda is on PATH" >&2
    exit 1
fi
conda activate "${CONDA_ENV_NAME:-foodatlas_pipeline}"

SCRIPT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "${SCRIPT_DIR}"

# Production run (all data, full 9 epochs, saves final model)
python -m src.lit2kg.biobert.train \
    --output_dir outputs/biobert_binary_prod \
    --production

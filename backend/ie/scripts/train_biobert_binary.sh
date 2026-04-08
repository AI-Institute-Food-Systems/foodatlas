#!/bin/bash
#SBATCH --job-name=biobert_binary
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gres=gpu:1
#SBATCH --mem=32G
#SBATCH --time=12:00:00

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "${SCRIPT_DIR}"

# Production run (all data, full 9 epochs, saves final model)
uv run python -m src.pipeline.filtering.biobert.train \
    --output_dir outputs/biobert_binary_prod \
    --production

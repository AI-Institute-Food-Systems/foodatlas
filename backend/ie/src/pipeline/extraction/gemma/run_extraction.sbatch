#!/usr/bin/env bash
#SBATCH --job-name=gemma4-ie
#SBATCH --partition=aifshpc1
#SBATCH --nodes=1
#SBATCH --gres=gpu:4
#SBATCH --cpus-per-task=16
#SBATCH --mem=180G
#SBATCH --time=12:00:00
#SBATCH --output=outputs/extraction/2026_06_02/slurm_%j.out
#SBATCH --error=outputs/extraction/2026_06_02/slurm_%j.err
set -eo pipefail

WORKDIR=/mnt/share/kaichixie/FA_monorepo/foodatlas/backend/ie
ENV=/mnt/share/kaichixie/miniconda3/envs/gemma4_infer

cd "$WORKDIR"
mkdir -p outputs/extraction/2026_06_02

export PATH="$ENV/bin:$PATH"
export CUDA_VISIBLE_DEVICES=0,1,2,3
export TOKENIZERS_PARALLELISM=false
export VLLM_LOGGING_LEVEL=WARNING

"$ENV/bin/python" -m src.pipeline.extraction.gemma.runner \
    --input_path  outputs/filtering/2026_06_02/filtered_sentences/information_extraction_input.tsv \
    --output_dir  outputs/extraction/2026_06_02 \
    --tensor_parallel_size 4

#!/bin/bash
#SBATCH --job-name=biobert_binary
#SBATCH --mail-type=ALL
#SBATCH --mail-user=kcxie@ucdavis.edu
#SBATCH --output=/mnt/share/kaichixie/foodatlas_pipeline/scripts/logs/%j.out
#SBATCH --error=/mnt/share/kaichixie/foodatlas_pipeline/scripts/logs/%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gres=gpu:1
#SBATCH --mem=32G
#SBATCH --time=12:00:00

source /mnt/share/kaichixie/miniconda3/etc/profile.d/conda.sh
conda activate foodatlas_pipeline

cd /mnt/share/kaichixie/foodatlas_pipeline

# Development run (train/val/test splits, saves best checkpoint by val F1)
# python -m src.Lit2_KG.biobert.train \
#     --output_dir outputs/biobert_binary

# Production run (all data, full 9 epochs, saves final model)
python -m src.Lit2_KG.biobert.train \
    --output_dir outputs/biobert_binary_prod \
    --production

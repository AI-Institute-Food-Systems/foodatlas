#!/usr/bin/env bash
# Fix the gemma4_infer conda env: replace the cu129 vLLM wheel with cu128.
#
# The env already has torch 2.10.0+cu128 and transformers 5.x with Gemma 4
# support. The only broken piece is the vLLM nightly that was built for cu129.
#
# Run once from any directory:
#   bash backend/ie/src/pipeline/extraction/gemma/setup_env.sh
set -euo pipefail

ENV=/mnt/share/kaichixie/miniconda3/envs/gemma4_infer
PY="$ENV/bin/python"
PIP="$ENV/bin/pip"

echo "=== Checking current state ==="
"$PY" -c "import torch; print('torch', torch.__version__)"
"$PY" -c "import transformers; print('transformers', transformers.__version__)"
echo "vLLM before:" && "$PIP" show vllm | grep Version || true

echo ""
echo "=== Uninstalling cu129 vLLM ==="
"$PIP" uninstall -y vllm

echo ""
echo "=== Installing vLLM nightly (cu128) ==="
UV_INDEX_STRATEGY=unsafe-best-match "$ENV/bin/uv" pip install \
    --python "$PY" \
    vllm --pre \
    --extra-index-url https://wheels.vllm.ai/nightly/cu128 \
    --extra-index-url https://download.pytorch.org/whl/cu128 \
    || \
"$PIP" install vllm --pre \
    --extra-index-url https://wheels.vllm.ai/nightly/cu128 \
    --extra-index-url https://download.pytorch.org/whl/cu128

echo ""
echo "=== Verifying ==="
"$PY" - <<'PY'
import vllm, torch, transformers
from transformers.models.auto.configuration_auto import CONFIG_MAPPING_NAMES as M
print("vllm        =", vllm.__version__)
print("torch       =", torch.__version__)
print("transformers=", transformers.__version__)
print("gemma4 ok   =", "gemma4" in M)

# Quick smoke-test: import vLLM's LLM class
from vllm import LLM, SamplingParams
print("vllm LLM    = OK")
PY

echo ""
echo "Done. Run inference with:"
echo "  PATH=/mnt/share/kaichixie/miniconda3/envs/gemma4_infer/bin:\$PATH \\"
echo "  CUDA_VISIBLE_DEVICES=0,1,2,3 \\"
echo "  /mnt/share/kaichixie/miniconda3/envs/gemma4_infer/bin/python \\"
echo "      -m src.pipeline.extraction.gemma.runner \\"
echo "      --input_path outputs/filtering/2026_06_02/filtered_sentences/information_extraction_input.tsv \\"
echo "      --output_dir  outputs/extraction/2026_06_02 \\"
echo "      --tensor_parallel_size 4"

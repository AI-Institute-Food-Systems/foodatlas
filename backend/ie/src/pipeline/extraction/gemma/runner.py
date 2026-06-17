"""Gemma 4 information extraction via vLLM.

Reads information_extraction_input.tsv, runs inference in resumable chunks,
and writes extraction_predicted.tsv compatible with parse_predictions.tsv_to_json.

Requires the gemma4_infer conda env (see setup_env.sh):
    /mnt/share/kaichixie/miniconda3/envs/gemma4_infer/bin/python

Run from backend/ie/:
    PATH=/mnt/share/kaichixie/miniconda3/envs/gemma4_infer/bin:$PATH \
    CUDA_VISIBLE_DEVICES=0,1,2,3 \
    /mnt/share/kaichixie/miniconda3/envs/gemma4_infer/bin/python \
        -m src.pipeline.extraction.gemma.runner \
        --input_path outputs/filtering/2026_06_02/filtered_sentences/information_extraction_input.tsv \
        --output_dir  outputs/extraction/2026_06_02 \
        --tensor_parallel_size 4
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm

log = logging.getLogger(__name__)

_DEFAULT_MODEL = "/mnt/share/kaichixie/model_weights/gemma-4-31B-it"
_DEFAULT_SYSTEM_PROMPT = "src/pipeline/extraction/prompts/system/v3.txt"
_PARAGRAPH_MARKER = "# PARAGRAPH"
_CONDA_ENV_BIN = "/mnt/share/kaichixie/miniconda3/envs/gemma4_infer/bin"


def _ensure_path() -> None:
    """Add conda env bin to PATH so worker processes can find ninja and other tools."""
    current = os.environ.get("PATH", "")
    if _CONDA_ENV_BIN not in current:
        os.environ["PATH"] = f"{_CONDA_ENV_BIN}:{current}"


def _load_system_instructions(prompt_path: str) -> str:
    """Return the instruction portion of the v3 prompt (everything before # PARAGRAPH)."""
    text = Path(prompt_path).read_text(encoding="utf-8")
    idx = text.find(_PARAGRAPH_MARKER)
    if idx == -1:
        return text.strip()
    return text[:idx].rstrip()


def _build_messages(sentences: list[str], system_instructions: str) -> list[list[dict]]:
    """Build one list of chat messages per sentence."""
    return [
        [
            {"role": "system", "content": system_instructions},
            {"role": "user", "content": sentence},
        ]
        for sentence in sentences
    ]


def _init_engine(
    model_dir: str,
    max_model_len: int,
    tensor_parallel_size: int,
    gpu_memory_utilization: float,
) -> object:
    from vllm import LLM

    return LLM(
        model=model_dir,
        dtype="bfloat16",
        max_model_len=max_model_len,
        tensor_parallel_size=tensor_parallel_size,
        gpu_memory_utilization=gpu_memory_utilization,
        enforce_eager=True,
        enable_prefix_caching=True,
        trust_remote_code=True,
    )


def _process_chunks(
    df: pd.DataFrame,
    engine: object,
    system_instructions: str,
    out_dir: Path,
    chunk_size: int,
    max_new_tokens: int,
    temperature: float,
) -> None:
    """Run inference chunk by chunk, skipping chunks that already exist on disk.

    After each chunk, re-writes extraction_predicted.tsv so it is always current.
    """
    from vllm import SamplingParams

    sampling_params = SamplingParams(max_tokens=max_new_tokens, temperature=temperature)

    for start in tqdm(range(0, len(df), chunk_size), desc="Chunks", unit="chunk"):
        chunk_path = out_dir / f"chunk_{start:07d}.tsv"
        if chunk_path.exists():
            log.info("Skipping chunk %d (already exists)", start)
            continue

        chunk = df.iloc[start : start + chunk_size]
        messages = _build_messages(chunk["sentence"].tolist(), system_instructions)

        outputs = engine.chat(messages=messages, sampling_params=sampling_params)  # type: ignore[union-attr]
        responses = [o.outputs[0].text for o in outputs]

        out_chunk = chunk.copy()
        out_chunk["response"] = responses
        out_chunk.to_csv(chunk_path, sep="\t", index=False)
        log.info(
            "Saved chunk %d-%d (%d rows) -> %s",
            start, start + len(chunk) - 1, len(chunk), chunk_path,
        )

        _aggregate(out_dir)


def _aggregate(out_dir: Path) -> Path:
    """Concatenate all chunk TSVs and write extraction_predicted.tsv."""
    chunk_files = sorted(out_dir.glob("chunk_*.tsv"))
    if not chunk_files:
        msg = f"No chunk files found in {out_dir}"
        raise FileNotFoundError(msg)

    frames = [pd.read_csv(f, sep="\t", dtype=str, keep_default_na=False) for f in chunk_files]
    agg = pd.concat(frames, ignore_index=True)

    # BioBERT score column is called 'answer'; tsv_to_json expects 'prob'
    if "answer" in agg.columns and "prob" not in agg.columns:
        agg = agg.rename(columns={"answer": "prob"})

    output_path = out_dir / "extraction_predicted.tsv"
    agg.to_csv(output_path, sep="\t", index=False)
    log.info("Aggregated %d rows -> %s", len(agg), output_path)
    return output_path


def run_gemma_extraction(
    *,
    input_path: str,
    output_dir: str,
    system_prompt_path: str = _DEFAULT_SYSTEM_PROMPT,
    model_dir: str = _DEFAULT_MODEL,
    max_new_tokens: int = 1024,
    temperature: float = 0.0,
    chunk_size: int = 1000,
    max_model_len: int = 8192,
    tensor_parallel_size: int = 4,
    gpu_memory_utilization: float = 0.90,
) -> str:
    """Run Gemma 4 extraction and return path to extraction_predicted.tsv.

    vLLM is imported lazily so this module loads cleanly in the project's uv env.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _ensure_path()

    system_instructions = _load_system_instructions(system_prompt_path)
    log.info("System prompt: %d chars", len(system_instructions))

    df = pd.read_csv(input_path, sep="\t", dtype=str, keep_default_na=False)
    log.info("Input: %d sentences from %s", len(df), input_path)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    engine = _init_engine(model_dir, max_model_len, tensor_parallel_size, gpu_memory_utilization)
    _process_chunks(df, engine, system_instructions, out_dir, chunk_size, max_new_tokens, temperature)

    return str(_aggregate(out_dir))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Gemma 4 extraction via vLLM.")
    p.add_argument("--input_path", required=True)
    p.add_argument("--output_dir", required=True)
    p.add_argument("--system_prompt", default=_DEFAULT_SYSTEM_PROMPT)
    p.add_argument("--model_dir", default=_DEFAULT_MODEL)
    p.add_argument("--max_new_tokens", type=int, default=1024)
    p.add_argument("--temperature", type=float, default=0.0)
    p.add_argument("--chunk_size", type=int, default=1000, help="Rows per saved chunk (for resume)")
    p.add_argument("--max_model_len", type=int, default=8192)
    p.add_argument("--tensor_parallel_size", type=int, default=4)
    p.add_argument("--gpu_memory_utilization", type=float, default=0.90)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    output_tsv = run_gemma_extraction(
        input_path=args.input_path,
        output_dir=args.output_dir,
        system_prompt_path=args.system_prompt,
        model_dir=args.model_dir,
        max_new_tokens=args.max_new_tokens,
        temperature=args.temperature,
        chunk_size=args.chunk_size,
        max_model_len=args.max_model_len,
        tensor_parallel_size=args.tensor_parallel_size,
        gpu_memory_utilization=args.gpu_memory_utilization,
    )
    print(f"Done: {output_tsv}")

"""EvaluationRunner — sample KG triples, judge them, write precision/recall/F1.

Runs after TRUST and before NEWSLETTER. Samples sentences from the KG's
literature triples (all sources matching ``source_filter``, e.g. ``lit2kg:`` =
every IE model, pooled and deduped per sentence), has a Claude agent adjudicate
each against its paper, and writes per-sentence audit rows plus an aggregate
metrics summary the newsletter can surface.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from .cost import Usage
from .judge import Judge
from .metrics import Counts, counts_from_adjudication, summarize
from .papers import load_paper
from .prompts import PROMPT_VERSION
from .sampler import SampledSentence, sample_sentences

if TYPE_CHECKING:
    from ...models.settings import EvaluationStageConfig, KGCSettings

logger = logging.getLogger(__name__)

DIR_EVALUATION = "evaluation"
FILE_SAMPLES = "evaluation_samples.parquet"
FILE_METRICS = "evaluation_metrics.json"


class EvaluationRunner:
    """Orchestrate the EVALUATION stage: sample, judge, persist metrics."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings
        self._cfg: EvaluationStageConfig = settings.pipeline.stages.evaluation

    def run(self) -> None:
        cfg = self._cfg
        kg_dir = Path(self._settings.kg_dir)

        samples = sample_sentences(kg_dir, cfg.source_filter, cfg.sample_size, cfg.seed)
        if cfg.limit:
            samples = samples[: cfg.limit]
        if not samples:
            logger.warning(
                "Evaluation: no KG triples match %r — skipping.", cfg.source_filter
            )
            return

        logger.info(
            "Evaluating %d KG sentences (sources=%s, judge=%s).",
            len(samples),
            cfg.source_filter,
            cfg.judge_model,
        )
        rows = self._judge_all(samples)
        if not rows:
            logger.warning("Evaluation: no sentences scored — nothing written.")
            return
        self._write(kg_dir, rows)

    def _judge_all(self, samples: list[SampledSentence]) -> list[dict]:
        judge = Judge(model=self._cfg.judge_model)
        results: list[dict | None] = [None] * len(samples)
        with ThreadPoolExecutor(max_workers=self._cfg.max_workers) as pool:
            futures = {
                pool.submit(self._judge_one, judge, s): i for i, s in enumerate(samples)
            }
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        return [r for r in results if r is not None]

    def _judge_one(self, judge: Judge, sample: SampledSentence) -> dict | None:
        paper = load_paper(
            sample.pmcid,
            self._cfg.bioc_cache_dir,
            fetch=self._cfg.fetch_missing_papers,
        )
        if paper is None:
            return None
        try:
            verdict, usage = judge.judge(paper, sample.sentence, sample.pairs)
        except Exception:
            logger.exception("Judge failed for PMCID %s", sample.pmcid)
            return None

        l1, l2 = counts_from_adjudication(verdict)
        model_triples = [
            {"food": f, "chemical": c, "concentration": conc}
            for f, c, conc in sample.pairs
        ]
        return {
            "pmcid": sample.pmcid,
            "sentence": sample.sentence,
            "n_model_triples": len(sample.pairs),
            "model_triples": json.dumps(model_triples),
            "matches": json.dumps([m.model_dump() for m in verdict.matches]),
            "model_only": json.dumps([p.model_dump() for p in verdict.model_only]),
            "gold_only": json.dumps([p.model_dump() for p in verdict.gold_only]),
            "l1_tp": l1.tp,
            "l1_fp": l1.fp,
            "l1_fn": l1.fn,
            "l2_tp": l2.tp,
            "l2_fp": l2.fp,
            "l2_fn": l2.fn,
            "judge_calls": usage.calls,
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "cache_read_tokens": usage.cache_read_tokens,
            "cache_write_tokens": usage.cache_write_tokens,
            "cost_usd": round(usage.cost_usd, 4),
        }

    def _write(self, kg_dir: Path, rows: list[dict]) -> None:
        out_dir = kg_dir / DIR_EVALUATION
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(out_dir / FILE_SAMPLES, index=False)

        l1 = _total(rows, "l1")
        l2 = _total(rows, "l2")
        usage = _total_usage(rows)
        n = len(rows)
        metrics = {
            "sources": self._cfg.source_filter,
            "judge_model": self._cfg.judge_model,
            "prompt_version": PROMPT_VERSION,
            "seed": self._cfg.seed,
            "n_sentences_scored": n,
            "created_at": datetime.now(UTC).isoformat(),
            "relation_level": summarize(l1),
            "strict_with_concentration": summarize(l2),
            "cost": {
                "total_usd": round(usage.cost_usd, 4),
                "per_sentence_usd": round(usage.cost_usd / n, 4) if n else 0.0,
                "judge_calls": usage.calls,
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "pricing": "Claude Opus 4.8 list price (USD/1M): in $5, out $25",
            },
            "recall_note": (
                "Recall is conditional on a sentence yielding >=1 KG triple; "
                "zero-extraction sentences are absent from the KG."
            ),
        }
        (out_dir / FILE_METRICS).write_text(json.dumps(metrics, indent=2) + "\n")
        self._write_readable(out_dir, metrics, rows)

        rl = metrics["relation_level"]
        logger.info(
            "Evaluation (relation level): P=%.3f R=%.3f F1=%.3f over %d sentences; "
            "cost $%.2f total ($%.4f/sentence).",
            rl["precision"],
            rl["recall"],
            rl["f1"],
            n,
            metrics["cost"]["total_usd"],
            metrics["cost"]["per_sentence_usd"],
        )

    @staticmethod
    def _write_readable(out_dir: Path, metrics: dict, rows: list[dict]) -> None:
        """Human-reviewable JSON: summary + per-sentence verdicts (parsed)."""
        samples = [
            {
                "pmcid": r["pmcid"],
                "sentence": r["sentence"],
                "model_extractions": json.loads(r["model_triples"]),
                "matches": json.loads(r["matches"]),
                "model_only": json.loads(r["model_only"]),
                "gold_only": json.loads(r["gold_only"]),
                "l1": {"tp": r["l1_tp"], "fp": r["l1_fp"], "fn": r["l1_fn"]},
                "l2": {"tp": r["l2_tp"], "fp": r["l2_fp"], "fn": r["l2_fn"]},
                "cost_usd": r["cost_usd"],
            }
            for r in rows
        ]
        payload = {"metrics": metrics, "samples": samples}
        (out_dir / "evaluation_samples.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
        )


def _total(rows: list[dict], level: str) -> Counts:
    total = Counts(0, 0, 0)
    for r in rows:
        total += Counts(r[f"{level}_tp"], r[f"{level}_fp"], r[f"{level}_fn"])
    return total


def _total_usage(rows: list[dict]) -> Usage:
    total = Usage()
    for r in rows:
        total += Usage(
            calls=r["judge_calls"],
            input_tokens=r["input_tokens"],
            output_tokens=r["output_tokens"],
            cache_read_tokens=r["cache_read_tokens"],
            cache_write_tokens=r["cache_write_tokens"],
        )
    return total

"""Render the one-sentence weekly release summary from newsletter.json.

Pairs with ``render_newsletter.py`` — both read the NEWSLETTER stage's
``newsletter.json`` (+ the evaluation-metrics JSON). Emits ``SUMMARY-<version>.md``:
the release's headline statistics, plus an LLM-as-a-judge extraction-quality line
(precision/recall/F1). ``version`` is used only for the output filename.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

_SENTENCE = (
    "Weekly update — {new_associations:,} new associations "
    "({new_food_chemical:,} new food-chemical associations across {foods:,} foods and "
    "{chemicals:,} chemicals) from {new_papers:,} new papers."
)
_QUALITY = (
    "\nExtraction quality (LLM-as-a-judge, {n} sample sentences): "
    "precision {precision:.2f}, recall {recall:.2f}, F1 {f1:.2f}."
)


def render_summary(payload: dict, metrics: dict | None = None) -> str:
    sentence = _SENTENCE.format(
        new_associations=payload["new_associations"],
        new_food_chemical=payload["new_food_chemical"],
        foods=payload["foods_touched"],
        chemicals=payload["chemicals_touched"],
        new_papers=payload["new_papers"],
    )
    if metrics:
        rel = metrics["relation_level"]
        sentence += _QUALITY.format(
            n=metrics["n_sentences_scored"],
            precision=rel["precision"],
            recall=rel["recall"],
            f1=rel["f1"],
        )
    return sentence


def main() -> int:
    args = parse_arguments()
    payload = json.loads(args.json.read_text(encoding="utf-8"))
    metrics = (
        json.loads(args.eval_metrics.read_text(encoding="utf-8"))
        if args.eval_metrics.is_file()
        else None
    )
    sentence = render_summary(payload, metrics)
    output = args.output or Path(f"SUMMARY-{args.version}.md")
    output.write_text(sentence + "\n", encoding="utf-8")
    print(f"Summary written to {output}")
    return 0


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="Release version, e.g. v4.2")
    parser.add_argument(
        "--json", type=Path, default=Path("outputs/kg/newsletter.json")
    )
    parser.add_argument(
        "--eval-metrics",
        type=Path,
        default=Path("outputs/kg/evaluation/evaluation_metrics.json"),
        help="Evaluation metrics JSON (precision/recall/F1); skipped if absent.",
    )
    parser.add_argument(
        "--output", type=Path, default=None, help="Default: SUMMARY-<version>.md"
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())

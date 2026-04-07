"""CLI entry point for information extraction pipeline."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import click
from src.runner import IEConfig, IERunner
from src.stages import IEStage

_STAGE_NAMES = [s.name.lower() for s in IEStage]


def _parse_stage(name: str) -> IEStage:
    """Parse a stage name or numeric index into an IEStage."""
    if name.isdigit():
        return IEStage(int(name))
    return IEStage[name.upper()]


def _resolve_stages(stage_arg: str | None) -> list[IEStage] | None:
    """Parse stage argument into a list of IEStages.

    Supports: single ("2"), range ("1:3"), or None for all.
    """
    if not stage_arg:
        return None
    if ":" in stage_arg:
        start_str, end_str = stage_arg.split(":", 1)
        start = _parse_stage(start_str)
        end = _parse_stage(end_str)
        return [s for s in IEStage if start.value <= s.value <= end.value]
    return [_parse_stage(stage_arg)]


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable DEBUG logging.")
def cli(verbose: bool) -> None:
    """FoodAtlas Information Extraction pipeline."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)-50s %(message)s",
    )


@cli.command()
@click.option(
    "--stages",
    default=None,
    help="Stage number or range (e.g. '3', '2:5'). Omit for all.",
)
@click.option(
    "--date",
    default=None,
    help="Run date tag (YYYY_MM_DD). Defaults to today.",
)
@click.option(
    "--model",
    default="gpt-5.2",
    help="LLM model for extraction step (default: gpt-5.2).",
)
@click.option(
    "--bioc-pmc-dir",
    envvar="BIOC_PMC_DIR",
    default="/mnt/data/shared/BioC-PMC",
    help="Path to local BioC-PMC corpus.",
)
@click.option(
    "--biobert-model-dir",
    default="outputs/biobert_binary_prod",
    help="Path to fine-tuned BioBERT model.",
)
@click.option(
    "--food-terms",
    default="data/food_terms.txt",
    help="Path to food query terms file.",
)
@click.option(
    "--threshold",
    type=float,
    default=0.99,
    help="BioBERT confidence threshold for sentence filtering.",
)
def run(
    stages: str | None,
    date: str | None,
    model: str,
    bioc_pmc_dir: str,
    biobert_model_dir: str,
    food_terms: str,
    threshold: float,
) -> None:
    """Run pipeline stages."""
    if date is None:
        date = datetime.now(tz=UTC).strftime("%Y_%m_%d")

    config = IEConfig(
        date=date,
        model=model,
        pipeline_dir=Path.cwd(),
        bioc_pmc_dir=bioc_pmc_dir,
        biobert_model_dir=biobert_model_dir,
        food_terms=food_terms,
        threshold=threshold,
    )
    runner = IERunner(config)
    runner.run(_resolve_stages(stages))


@cli.command("stages")
def list_stages() -> None:
    """List available pipeline stages."""
    for s in IEStage:
        click.echo(f"  {s.value}: {s.name}")


if __name__ == "__main__":
    cli()

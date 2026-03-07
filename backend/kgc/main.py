"""CLI entry point for knowledge graph construction."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import click
from src.models.settings import KGCSettings
from src.pipeline.runner import PipelineRunner
from src.pipeline.stages import PipelineStage

_STAGE_NAMES = [s.name.lower() for s in PipelineStage]


def _resolve_stages(stage_names: tuple[str, ...]) -> list[PipelineStage] | None:
    """Convert CLI stage names to PipelineStage enums, or None for all."""
    if not stage_names:
        return None
    return [PipelineStage[name.upper()] for name in stage_names]


@click.group()
@click.option(
    "--config",
    type=click.Path(exists=True),
    default=None,
    help="Path to config JSON (overrides defaults.json).",
)
@click.option(
    "--output-format",
    type=click.Choice(["json", "jsonl", "parquet"]),
    default=None,
    help="Output serialization format.",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable DEBUG logging.")
@click.pass_context
def cli(
    ctx: click.Context,
    config: str | None,
    output_format: str | None,
    verbose: bool,
) -> None:
    """FoodAtlas Knowledge Graph Construction pipeline."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    kwargs: dict[str, str] = {}
    if config:
        with Path(config).open() as f:
            data: dict[str, str] = json.load(f)
            kwargs.update(data)
    if output_format:
        kwargs["output_format"] = output_format

    ctx.ensure_object(dict)
    ctx.obj["settings"] = KGCSettings.model_validate(kwargs)


@cli.command()
@click.option(
    "--stage",
    "stages",
    multiple=True,
    type=click.Choice(_STAGE_NAMES, case_sensitive=False),
    help="Stage to run (repeatable). Omit for all stages.",
)
@click.pass_context
def run(ctx: click.Context, stages: tuple[str, ...]) -> None:
    """Run pipeline stages."""
    settings: KGCSettings = ctx.obj["settings"]
    runner = PipelineRunner(settings)
    runner.run(_resolve_stages(stages))


@cli.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """Shortcut: run KG initialization only."""
    settings: KGCSettings = ctx.obj["settings"]
    runner = PipelineRunner(settings)
    runner.run([PipelineStage.KG_INIT])


if __name__ == "__main__":
    cli()

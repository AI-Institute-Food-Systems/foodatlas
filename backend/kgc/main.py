"""CLI entry point for knowledge graph construction."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import click
from src.models.settings import KGCSettings
from src.pipeline.ingest.runner import ALL_ADAPTERS
from src.pipeline.kg_diff.compare import run_diff
from src.pipeline.kg_diff.load_old import load_old_kg
from src.pipeline.kg_diff.report import format_report
from src.pipeline.runner import PipelineRunner
from src.pipeline.stages import PipelineStage

_STAGE_NAMES = [s.name.lower() for s in PipelineStage]
_VALID_STAGES = _STAGE_NAMES + [str(s.value) for s in PipelineStage]
_VALID_SOURCES = [cls().source_id for cls in ALL_ADAPTERS]


def _parse_stage(name: str) -> PipelineStage:
    """Parse a stage name or numeric index into a PipelineStage."""
    if name.isdigit():
        return PipelineStage(int(name))
    return PipelineStage[name.upper()]


def _resolve_stages(stage_arg: str | None) -> list[PipelineStage] | None:
    """Parse stage argument into a list of PipelineStages.

    Supports: single ("2"), range ("1:3"), or None for all.
    """
    if not stage_arg:
        return None
    if ":" in stage_arg:
        start_str, end_str = stage_arg.split(":", 1)
        start = _parse_stage(start_str)
        end = _parse_stage(end_str)
        return [s for s in PipelineStage if start.value <= s.value <= end.value]
    return [_parse_stage(stage_arg)]


@click.group()
@click.option(
    "--config",
    type=click.Path(exists=True),
    default=None,
    help="Path to config JSON (overrides defaults.json).",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable DEBUG logging.")
@click.pass_context
def cli(
    ctx: click.Context,
    config: str | None,
    verbose: bool,
) -> None:
    """FoodAtlas Knowledge Graph Construction pipeline."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)-50s %(message)s",
    )

    kwargs: dict[str, str] = {}
    if config:
        with Path(config).open() as f:
            data: dict[str, str] = json.load(f)
            kwargs.update(data)

    ctx.ensure_object(dict)
    ctx.obj["settings"] = KGCSettings.model_validate(kwargs)


@cli.command()
@click.option(
    "--stages",
    default=None,
    help="Stage name or number, or range (e.g. 1:3). Omit for all.",
)
@click.option(
    "--source",
    "sources",
    multiple=True,
    type=click.Choice(_VALID_SOURCES, case_sensitive=False),
    help="Source adapter to run (repeatable). Only applies to ingest stage.",
)
@click.pass_context
def run(
    ctx: click.Context,
    stages: str | None,
    sources: tuple[str, ...],
) -> None:
    """Run pipeline stages."""
    settings: KGCSettings = ctx.obj["settings"]
    runner = PipelineRunner(settings)
    source_list = list(sources) if sources else None
    runner.run(_resolve_stages(stages), sources=source_list)


@cli.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """Shortcut: run ingest and entity resolution."""
    settings: KGCSettings = ctx.obj["settings"]
    runner = PipelineRunner(settings)
    runner.run([PipelineStage.INGEST, PipelineStage.ENTITIES])


@cli.command("diff")
@click.option(
    "--output",
    type=click.Path(),
    default=None,
    help="Write report to file instead of stdout.",
)
@click.pass_context
def diff_cmd(ctx: click.Context, output: str | None) -> None:
    """Compare old v3.3 KG with current KG."""
    settings: KGCSettings = ctx.obj["settings"]
    old_kg = load_old_kg(settings.data_dir)
    result = run_diff(old_kg, settings.kg_dir)
    report = format_report(result)
    if output:
        Path(output).write_text(report)
        click.echo(f"Report written to {output}")
    else:
        click.echo(report)


if __name__ == "__main__":
    cli()

"""CLI entry point for information extraction pipeline."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import click
from src.models.settings import IESettings
from src.pipeline.runner import IERunner
from src.pipeline.stages import IEStage


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
    """FoodAtlas Information Extraction pipeline."""
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
    ctx.obj["settings"] = IESettings.model_validate(kwargs)


@cli.command()
@click.option(
    "--stages",
    default=None,
    help="Stage number or range (e.g. '3', '2:5'). Omit for all.",
)
@click.pass_context
def run(ctx: click.Context, stages: str | None) -> None:
    """Run pipeline stages."""
    settings: IESettings = ctx.obj["settings"]
    runner = IERunner(settings)
    runner.run(_resolve_stages(stages))


@cli.command("stages")
def list_stages() -> None:
    """List available pipeline stages."""
    for s in IEStage:
        click.echo(f"  {s.value}: {s.name}")


if __name__ == "__main__":
    cli()

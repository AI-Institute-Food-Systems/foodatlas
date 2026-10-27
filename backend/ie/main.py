"""CLI entry point for information extraction pipeline."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import click
from src.models.settings import IESettings
from src.pipeline.runner import IERunner
from src.pipeline.search.pending_ledger import LEDGER_NAME, PendingLedger
from src.pipeline.search.pending_rebuild import rebuild as rebuild_ledger
from src.pipeline.search.runner import _scan_cached_pmcids
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


@cli.command("pending")
@click.option(
    "--rebuild",
    is_flag=True,
    help="Rebuild outputs/search/pending.tsv from past run dirs and the BioC cache.",
)
@click.pass_context
def pending(ctx: click.Context, rebuild: bool) -> None:
    """Show, or --rebuild, the ledger of articles still waiting for PMC full text."""
    settings: IESettings = ctx.obj["settings"]
    outputs = Path.cwd() / "outputs"
    path = outputs / "search" / LEDGER_NAME
    if rebuild:
        bioc_dir = Path(settings.bioc_pmc_dir)
        ledger = rebuild_ledger(
            outputs / "search",
            outputs / "extraction",
            bioc_dir,
            _scan_cached_pmcids(bioc_dir),
            settings.resolved_date,
        )
        ledger.save()
    else:
        ledger = PendingLedger.load(path)
    waiting = sum(1 for r in ledger.rows.values() if not r.handed_to)
    click.echo(
        f"{path}: {len(ledger.rows)} rows — {waiting} awaiting PMC full text, "
        f"{len(ledger.rows) - waiting} handed to a run"
    )


if __name__ == "__main__":
    cli()

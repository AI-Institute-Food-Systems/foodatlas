"""CLI entry point for knowledge graph construction."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import click
import pandas as pd
from src.models.settings import KGCSettings
from src.pipeline.ingest.runner import ALL_ADAPTERS
from src.pipeline.report.format import format_changelog, format_report
from src.pipeline.report.load_old import load_old_kg
from src.pipeline.report.runner import run_diff
from src.pipeline.runner import PipelineRunner
from src.pipeline.stages import PipelineStage
from src.stores.schema import DIR_DIAGNOSTICS
from src.utils.orphans import write_orphans_jsonl
from src.utils.unclassified import write_unclassified_jsonl

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


@cli.command("diagnostics")
@click.pass_context
def diagnostics_cmd(ctx: click.Context) -> None:
    """Regenerate KGC diagnostics (orphans, unclassified) from current KG."""
    settings: KGCSettings = ctx.obj["settings"]
    kg_path = Path(settings.kg_dir)
    ents = pd.read_parquet(
        kg_path / "entities.parquet",
        columns=["foodatlas_id", "entity_type", "common_name"],
    ).set_index("foodatlas_id")
    trips = pd.read_parquet(
        kg_path / "triplets.parquet",
        columns=["head_id", "relationship_id", "tail_id", "attestation_ids"],
    )
    trips["attestation_ids"] = trips["attestation_ids"].apply(
        lambda x: json.loads(x) if isinstance(x, str) else (x or [])
    )
    diag_dir = kg_path / DIR_DIAGNOSTICS

    orphans_out = diag_dir / "kgc_orphans.jsonl"
    n_orphans = write_orphans_jsonl(ents, trips, orphans_out)
    click.echo(f"Wrote {n_orphans} orphan entities to {orphans_out}")

    unclass_out = diag_dir / "kgc_unclassified.jsonl"
    n_unclass = write_unclassified_jsonl(ents, trips, unclass_out)
    click.echo(f"Wrote {n_unclass} unclassified entities to {unclass_out}")


@cli.command("report")
@click.option(
    "--output",
    type=click.Path(),
    default=None,
    help="Write report to file. Defaults to '<kg_dir>/CHANGELOG.md' for "
    "markdown format, stdout for text format. Pass '-' to force stdout.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["markdown", "text"], case_sensitive=False),
    default="markdown",
    help="Output format: 'markdown' (default) for release-notes bundled "
    "into the release zip, 'text' for the plaintext operator report.",
)
@click.pass_context
def report_cmd(ctx: click.Context, output: str | None, fmt: str) -> None:
    """Compare old v3.3 KG with current KG and emit a release report."""
    settings: KGCSettings = ctx.obj["settings"]
    old_kg = load_old_kg(settings.data_dir)
    result = run_diff(old_kg, settings.kg_dir)
    is_markdown = fmt.lower() != "text"
    rendered = format_changelog(result) if is_markdown else format_report(result)

    if output is None and is_markdown:
        output = str(Path(settings.kg_dir) / "CHANGELOG.md")

    if output and output != "-":
        Path(output).write_text(rendered)
        click.echo(f"Report written to {output}")
    else:
        click.echo(rendered)


if __name__ == "__main__":
    cli()

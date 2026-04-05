"""CLI entry point for the database layer."""

import logging
from pathlib import Path

import click
from src.config import DBSettings
from src.engine import create_sync_engine
from src.etl.loader import load_kg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)


@click.group()
def cli() -> None:
    """FoodAtlas database management CLI."""


@cli.command()
@click.option(
    "--parquet-dir",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to KGC output directory containing parquet files.",
)
def load(parquet_dir: Path) -> None:
    """Load KGC parquet output into PostgreSQL."""
    settings = DBSettings()
    engine = create_sync_engine(settings)
    with engine.begin() as conn:
        load_kg(conn, parquet_dir)
    click.echo("Done.")


if __name__ == "__main__":
    cli()

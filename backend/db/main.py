"""CLI entry point for the database layer."""

import logging
import tempfile
from pathlib import Path

import click
from src.config import DBSettings
from src.engine import create_sync_engine
from src.etl.loader import load_kg, refresh_materialized_views
from src.etl.s3_sync import download_s3_prefix, is_s3_uri

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)

_DEFAULT_PARQUET_DIR = Path(__file__).resolve().parent.parent / "kgc" / "outputs" / "kg"


@click.group()
def cli() -> None:
    """FoodAtlas database management CLI."""


@cli.command()
@click.option(
    "--parquet-dir",
    type=str,
    default=str(_DEFAULT_PARQUET_DIR),
    show_default=True,
    help=(
        "Path to KGC output directory containing parquet files. Accepts a "
        "local path or an s3:// URI (e.g. s3://bucket/kg). S3 URIs are "
        "downloaded to a temporary directory first."
    ),
)
def load(parquet_dir: str) -> None:
    """Load KGC parquet output into PostgreSQL."""
    settings = DBSettings()
    engine = create_sync_engine(settings)

    if is_s3_uri(parquet_dir):
        with tempfile.TemporaryDirectory(prefix="foodatlas-s3-") as tmp:
            local_dir = Path(tmp)
            download_s3_prefix(parquet_dir, local_dir)
            with engine.connect() as conn:
                load_kg(conn, local_dir)
    else:
        local_path = Path(parquet_dir)
        if not local_path.exists():
            msg = f"Parquet directory does not exist: {local_path}"
            raise click.BadParameter(msg, param_hint="--parquet-dir")
        with engine.connect() as conn:
            load_kg(conn, local_path)

    click.echo("Done.")


@cli.command("refresh")
def refresh() -> None:
    """Rebuild materialized views from existing base tables.

    Skips parquet read and base table inserts. Use this when iterating on
    materializer logic without touching the underlying KG data.
    """
    settings = DBSettings()
    engine = create_sync_engine(settings)
    with engine.connect() as conn:
        refresh_materialized_views(conn)
    click.echo("Done.")


if __name__ == "__main__":
    cli()

"""CLI entry point for the database layer."""

import logging
import tempfile
from pathlib import Path

import click
from sqlalchemy import text
from src.config import DBSettings
from src.engine import create_sync_engine
from src.etl.loader import load_kg, load_trust_only, refresh_materialized_views
from src.etl.s3_sync import download_s3_prefix, is_s3_uri

_LOAD_SCOPES = ["trust"]

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
@click.option(
    "--only",
    type=click.Choice(_LOAD_SCOPES),
    default=None,
    help=(
        "Load only the named subset, skipping the full ETL "
        "(no schema drop, no base bulk-inserts, no MV refresh). "
        "Currently supported: 'trust' — upserts trust_signals.parquet "
        "into base_trust_signals (TrustBase, separate metadata)."
    ),
)
def load(parquet_dir: str, only: str | None) -> None:
    """Load KGC parquet output into PostgreSQL."""
    settings = DBSettings()
    engine = create_sync_engine(settings)
    loader = load_trust_only if only == "trust" else load_kg

    if is_s3_uri(parquet_dir):
        with tempfile.TemporaryDirectory(prefix="foodatlas-s3-") as tmp:
            local_dir = Path(tmp)
            download_s3_prefix(parquet_dir, local_dir)
            with engine.connect() as conn:
                loader(conn, local_dir)
    else:
        local_path = Path(parquet_dir)
        if not local_path.exists():
            msg = f"Parquet directory does not exist: {local_path}"
            raise click.BadParameter(msg, param_hint="--parquet-dir")
        with engine.connect() as conn:
            loader(conn, local_path)

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


# Ordered list of (description, sql) for the bioact-perf migration.
#
# Ordering matters for concurrency safety against a live API:
#   1. SET lock_timeout — fail fast if we can't get a lock, instead of
#      starving in the queue (a queued ALTER blocks every subsequent
#      reader behind it, degrading the API for the whole wait).
#   2. CREATE INDEX CONCURRENTLY for the read-side indexes — these take
#      only ShareUpdateExclusiveLock, so concurrent SELECT/UPDATE on the
#      MVs is unaffected. Restores ~all of the missing sort/join perf
#      even if the ALTER stage below later fails.
#   3. ALTER + UPDATE + the n_foods-dependent composite index last —
#      these need heavier locks (AccessExclusive for ALTER) and depend
#      on the new column. If lock_timeout fires here, indexes from (2)
#      have already shipped and the migration can be re-run off-hours.
#
# All statements are idempotent (IF NOT EXISTS / re-runnable UPDATE) so
# re-running after a partial failure is safe.
_BIOACT_PERF_MIGRATION: list[tuple[str, str]] = [
    (
        "session: fail any blocked DDL after 30s instead of starving",
        "SET lock_timeout = '30s'",
    ),
    (
        "FCC(chemical_foodatlas_id) — speeds n_foods + inferred-bio join",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_mv_fcc_chemical_id "
        "ON mv_food_chemical_composition(chemical_foodatlas_id)",
    ),
    (
        "CB(chemical_foodatlas_id) — speeds inferred-bioactivities join",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_mv_cb_chemical_id "
        "ON mv_chemical_bioactivity(chemical_foodatlas_id)",
    ),
    (
        "composite CB(bioactivity_name, measurement_count) — default sort",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_mv_cb_bio_mcount "
        "ON mv_chemical_bioactivity(bioactivity_name, measurement_count)",
    ),
    (
        "composite CB(chemical_name, measurement_count) — chem-bio default sort",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_mv_cb_chem_mcount "
        "ON mv_chemical_bioactivity(chemical_name, measurement_count)",
    ),
    (
        "composite FB(food_name, measurement_count) — /food/bioactivities sort",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_mv_fb_food_mcount "
        "ON mv_food_bioactivity(food_name, measurement_count)",
    ),
    (
        "composite FB(bioactivity_name, measurement_count) — bio-foods sort",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_mv_fb_bio_mcount "
        "ON mv_food_bioactivity(bioactivity_name, measurement_count)",
    ),
    (
        "add mv_chemical_bioactivity.n_foods column (default 0)",
        "ALTER TABLE mv_chemical_bioactivity "
        "ADD COLUMN IF NOT EXISTS n_foods INTEGER DEFAULT 0",
    ),
    (
        "backfill n_foods from distinct foods in mv_food_chemical_composition",
        "UPDATE mv_chemical_bioactivity cb "
        "SET n_foods = COALESCE(nf.n_foods, 0) "
        "FROM ("
        "  SELECT chemical_foodatlas_id, "
        "         COUNT(DISTINCT food_foodatlas_id) AS n_foods "
        "  FROM mv_food_chemical_composition "
        "  GROUP BY chemical_foodatlas_id"
        ") nf "
        "WHERE cb.chemical_foodatlas_id = nf.chemical_foodatlas_id",
    ),
    (
        "composite CB(bioactivity_name, n_foods) — sort by # foods column",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_mv_cb_bio_nfoods "
        "ON mv_chemical_bioactivity(bioactivity_name, n_foods)",
    ),
]


@cli.command("migrate-bioact-perf")
def migrate_bioact_perf() -> None:
    """One-shot migration: add n_foods column + bioactivity perf indexes.

    Idempotent — uses ADD COLUMN IF NOT EXISTS and CREATE INDEX
    CONCURRENTLY IF NOT EXISTS throughout. Brings an already-loaded RDS
    to the schema the PR introducing materialised n_foods + composite
    indexes expects, without waiting for a full ``db load`` rebuild.
    Triggered via the same Fargate task definition as ``db load`` — see
    ``infra/aws/scripts/run-migration.sh``.

    AUTOCOMMIT is required because CREATE INDEX CONCURRENTLY refuses to
    run inside an explicit transaction. It also means each statement
    commits independently, so a later failure (e.g. ALTER hitting
    ``lock_timeout``) doesn't roll back the indexes that already shipped.
    """
    settings = DBSettings()
    engine = create_sync_engine(settings)
    logger = logging.getLogger("migrate-bioact-perf")
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for description, sql in _BIOACT_PERF_MIGRATION:
            logger.info(">>> %s", description)
            conn.execute(text(sql))
    click.echo("Done.")


if __name__ == "__main__":
    cli()

"""CLI entry point for the database layer."""

import logging
import tempfile
from pathlib import Path

import click
from sqlalchemy import Connection, Engine, text
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


def _vacuum_analyze(engine: Engine) -> None:
    """Refresh planner stats and visibility maps after a bulk reload.

    TRUNCATE + COPY leaves the MVs with no statistics and an empty
    visibility map until autovacuum gets to them, so the first queries
    after a load can pick bad plans and can't use index-only scans.
    VACUUM refuses to run inside a transaction, hence AUTOCOMMIT.
    """
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("VACUUM ANALYZE"))


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

    _vacuum_analyze(engine)
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
    _vacuum_analyze(engine)
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


# The sort indexes with the row key appended, so the API's tiebreak
# (ORDER BY count, <ids>) reads one index range instead of sorting every
# tied row. Builds each new index before dropping the one it replaces, so
# no sort is ever left without an index. Idempotent.
_SORT_KEY_INDEXES = [
    (
        "ix_mv_cb_bio_mcount",
        "mv_chemical_bioactivity",
        "bioactivity_name, measurement_count",
    ),
    (
        "ix_mv_cb_chem_mcount",
        "mv_chemical_bioactivity",
        "chemical_name, measurement_count",
    ),
    ("ix_mv_cb_bio_nfoods", "mv_chemical_bioactivity", "bioactivity_name, n_foods"),
    ("ix_mv_fb_food_mcount", "mv_food_bioactivity", "food_name, measurement_count"),
    (
        "ix_mv_fb_bio_mcount",
        "mv_food_bioactivity",
        "bioactivity_name, measurement_count",
    ),
]
_ROW_KEY = {
    "mv_chemical_bioactivity": "chemical_foodatlas_id, bioactivity_foodatlas_id",
    "mv_food_bioactivity": "food_foodatlas_id, bioactivity_foodatlas_id",
}


def _index_valid(conn: Connection, name: str) -> bool | None:
    """pg_index.indisvalid for ``name``; None when the index doesn't exist."""
    return conn.execute(
        text("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:n)"),
        {"n": name},
    ).scalar()


@cli.command("migrate-sort-key-indexes")
def migrate_sort_key_indexes() -> None:
    """One-shot: add the row key to the bioactivity sort indexes.

    Brings a loaded RDS to the indexes in ``models/views.py`` without a
    full ``db load``. Run via ``infra/aws/scripts/run-migration.sh``.
    """
    engine = create_sync_engine(DBSettings())
    logger = logging.getLogger("migrate-sort-key-indexes")
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("SET lock_timeout = '30s'"))
        for old, table, cols in _SORT_KEY_INDEXES:
            new = f"{old}_key"
            logger.info(">>> %s", new)
            # A failed CONCURRENTLY build leaves an INVALID index that
            # IF NOT EXISTS would skip; rebuild it instead.
            if _index_valid(conn, new) is False:
                conn.execute(text(f"DROP INDEX CONCURRENTLY IF EXISTS {new}"))
            conn.execute(
                text(
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {new} "
                    f"ON {table}({cols}, {_ROW_KEY[table]})"
                )
            )
            if not _index_valid(conn, new):
                msg = f"{new} is not valid; kept {old}. Re-run the migration."
                raise click.ClickException(msg)
            conn.execute(text(f"DROP INDEX CONCURRENTLY IF EXISTS {old}"))
    _vacuum_analyze(engine)
    click.echo("Done.")


if __name__ == "__main__":
    cli()

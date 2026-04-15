"""ETL orchestrator: load KGC parquet output into PostgreSQL."""

import logging
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Connection

from ..models import Base
from . import parquet_reader
from .bulk_insert import bulk_copy
from .materializer import refresh_all
from .materializer_search import refresh_search

logger = logging.getLogger(__name__)


def _recreate_schema(conn: Connection) -> None:
    """Drop all tables and recreate from ORM models."""
    logger.info("Recreating database schema...")
    Base.metadata.drop_all(bind=conn)
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    Base.metadata.create_all(bind=conn)
    conn.commit()


def load_kg(conn: Connection, parquet_dir: Path) -> None:
    """Full ETL: read parquet, load base tables, materialize API tables."""
    kg_dir = parquet_dir.resolve()
    logger.info("Loading KG from %s", kg_dir)

    # 1. Read parquet files
    logger.info("Reading parquet files...")
    entities_df = parquet_reader.read_entities(kg_dir)
    relationships_df = parquet_reader.read_relationships(kg_dir)
    triplets_df = parquet_reader.read_triplets(kg_dir)
    evidence_df = parquet_reader.read_evidence(kg_dir)
    attestations_df = parquet_reader.read_attestations(kg_dir)

    # 2. Recreate schema from ORM models
    _recreate_schema(conn)

    # 3. Bulk insert base tables (FK order: entities first)
    logger.info("Inserting entities (%d rows)...", len(entities_df))
    bulk_copy(
        conn,
        "base_entities",
        entities_df,
        [
            "foodatlas_id",
            "entity_type",
            "common_name",
            "scientific_name",
            "synonyms",
            "external_ids",
            "attributes",
        ],
    )

    logger.info("Inserting relationships (%d rows)...", len(relationships_df))
    bulk_copy(
        conn,
        "relationships",
        relationships_df,
        [
            "foodatlas_id",
            "name",
        ],
    )

    logger.info("Inserting triplets (%d rows)...", len(triplets_df))
    bulk_copy(
        conn,
        "base_triplets",
        triplets_df,
        [
            "head_id",
            "relationship_id",
            "tail_id",
            "source",
            "attestation_ids",
        ],
    )

    logger.info("Inserting evidence (%d rows)...", len(evidence_df))
    bulk_copy(
        conn,
        "base_evidence",
        evidence_df,
        [
            "evidence_id",
            "source_type",
            "reference",
        ],
    )

    logger.info("Inserting attestations (%d rows)...", len(attestations_df))
    bulk_copy(
        conn,
        "base_attestations",
        attestations_df,
        [
            "attestation_id",
            "evidence_id",
            "source",
            "head_name_raw",
            "tail_name_raw",
            "conc_value",
            "conc_unit",
            "conc_value_raw",
            "conc_unit_raw",
            "food_part",
            "food_processing",
            "filter_score",
            "validated",
            "validated_correct",
            "head_candidates",
            "tail_candidates",
        ],
    )
    conn.commit()

    # 4. Materialize API tables
    refresh_materialized_views(conn)
    logger.info("ETL complete.")


def refresh_materialized_views(conn: Connection) -> None:
    """Rebuild all materialized views from existing base tables."""
    logger.info("Materializing entity views + composition tables...")
    refresh_all(conn)
    logger.info("Materializing search + statistics...")
    refresh_search(conn)

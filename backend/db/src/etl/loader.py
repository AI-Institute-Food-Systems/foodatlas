"""ETL orchestrator: load KGC parquet output into PostgreSQL."""

import logging
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection

from ..models import Base, BaseTrustSignal, TrustBase
from . import parquet_reader
from .bulk_insert import bulk_copy
from .materializer import refresh_all
from .materializer_search import refresh_search

logger = logging.getLogger(__name__)

# Postgres caps a single statement at 65535 bind parameters (16-bit). With
# ~10 columns per trust-signal row, batches of 5000 stay well under the cap
# (50000 params) with safety margin for any future column additions.
_UPSERT_BATCH_SIZE = 5000


def _recreate_schema(conn: Connection) -> None:
    """Drop all tables and recreate from ORM models."""
    logger.info("Recreating database schema...")
    Base.metadata.drop_all(bind=conn)
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    Base.metadata.create_all(bind=conn)
    conn.commit()


def _load_trust_signals(conn: Connection, kg_dir: Path) -> None:
    """Idempotently create base_trust_signals (separate Base) and upsert rows.

    Trust signals live on :class:`TrustBase`, distinct from :class:`Base`, so
    ``_recreate_schema``'s ``Base.metadata.drop_all`` does not wipe them.
    Re-running ``db load`` upserts (last-write-wins on signal_id) so a
    successful retry from KGC overwrites a prior error row.
    """
    TrustBase.metadata.create_all(bind=conn)
    df = parquet_reader.read_trust_signals(kg_dir)
    if df is None or df.empty:
        logger.info("No trust_signals.parquet to load (skipping).")
        return

    rows = df.to_dict(orient="records")
    # Chunk to stay under Postgres' 65535-bind-parameter cap per statement.
    for start in range(0, len(rows), _UPSERT_BATCH_SIZE):
        chunk = rows[start : start + _UPSERT_BATCH_SIZE]
        stmt = pg_insert(BaseTrustSignal).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=[BaseTrustSignal.signal_id],
            set_={
                "score": stmt.excluded.score,
                "reason": stmt.excluded.reason,
                "error_text": stmt.excluded.error_text,
                "model": stmt.excluded.model,
                "created_at": stmt.excluded.created_at,
            },
        )
        conn.execute(stmt)
    conn.commit()
    logger.info("Upserted %d trust-signal rows.", len(rows))


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
    attestations_bioactivity_df = parquet_reader.read_attestations_bioactivity(kg_dir)
    bioassays_df = parquet_reader.read_bioassays(kg_dir)
    efficacy_df = parquet_reader.read_food_chemical_efficacy(kg_dir)
    disease_bridge_df = parquet_reader.read_bioactivity_disease(kg_dir)
    disease_targets_df = parquet_reader.read_bioactivity_disease_targets(kg_dir)

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

    # Bioactivity measurements — present only when the KG includes the
    # bioactivity source. r5/r6 triplets' attestation_ids resolve here.
    if attestations_bioactivity_df is not None:
        logger.info(
            "Inserting bioactivity measurements (%d rows)...",
            len(attestations_bioactivity_df),
        )
        bulk_copy(
            conn,
            "base_attestations_bioactivity",
            attestations_bioactivity_df,
            [
                "bioactivity_metadata_id",
                "exhibit_type",
                "source_assay_id",
                "reported_activity_outcome",
                "evidence_endpoint_type",
                "evidence_relation",
                "potency_value",
                "potency_unit",
                "efficacy_zeroactivity",
                "efficacy_infiniteactivity",
                "efficacy_logac50_value",
                "efficacy_hillslope",
                "evidence_source",
                "evidence_type",
                "evidence_fit_r2",
                "evidence_fit_curveclass",
            ],
        )

    # Bioassay dimension — the assay-level metadata joined onto measurements by
    # source_assay_id. Present only alongside the bioactivity measurements.
    if bioassays_df is not None:
        logger.info("Inserting bioassays (%d rows)...", len(bioassays_df))
        bulk_copy(
            conn,
            "base_bioassays",
            bioassays_df,
            [
                "source_assay_id",
                "source",
                "n_measurements",
                "assay_description",
                "target_id",
                "target_name",
                "target_organism",
                "target_uniprot",
                "target_entrez_gene",
                "bioactivity_ids",
                "last_modified",
            ],
        )

    # Food-chemical-bioactivity efficacy — present only alongside the bioactivity
    # source. Resolved (cid→chemical, E300→concept) into mv at materialize time.
    if efficacy_df is not None:
        logger.info("Inserting food-chemical efficacy (%d rows)...", len(efficacy_df))
        bulk_copy(
            conn,
            "base_food_chemical_efficacy",
            efficacy_df,
            [
                "foodatlas_id",
                "cid",
                "bioactivity_id",
                "food_name",
                "food_conc_mg_per_100g",
                "food_conc_mass_fraction_pct",
                "conc_quality_flag",
                "molecular_weight",
                "food_conc_m",
                "food_conc_logm",
                "rep_source_assay_id",
                "endpoint_type",
                "endpoint_class",
                "curve_method",
                "logac50",
                "hillslope",
                "zeroactivity",
                "infiniteactivity",
                "n_curves",
                "n_curves_4param",
                "curve_agreement",
                "ac50_spread_log",
                "logac50_median",
                "logac50_min",
                "logac50_max",
                "dose_over_ac50_log",
                "conc_vs_ac50",
                "efficacy_fraction",
                "efficacy_response",
                "saturated",
            ],
        )

    # Bioactivity disease↔assay bridge + target genes — the input to the inferred
    # chemical↔disease associations (mv_chemical_disease_bioactivity).
    if disease_bridge_df is not None:
        logger.info(
            "Inserting bioactivity-disease bridge (%d rows)...", len(disease_bridge_df)
        )
        bulk_copy(
            conn,
            "base_bioactivity_disease",
            disease_bridge_df,
            [
                "disease_mesh_id",
                "source_assay_id",
                "relationship",
                "bioactivity_disease_metadata_id",
            ],
        )
    if disease_targets_df is not None:
        logger.info(
            "Inserting bioactivity-disease targets (%d rows)...",
            len(disease_targets_df),
        )
        bulk_copy(
            conn,
            "base_bioactivity_disease_targets",
            disease_targets_df,
            ["bioactivity_disease_metadata_id", "target_ids"],
        )
    conn.commit()

    # 4. Trust signals (optional, opt-in via KGC trust stage)
    _load_trust_signals(conn, kg_dir)

    # 5. Materialize API tables
    refresh_materialized_views(conn)
    logger.info("ETL complete.")


def load_trust_only(conn: Connection, parquet_dir: Path) -> None:
    """Upsert only ``trust_signals.parquet`` into ``base_trust_signals``.

    Skips the full ETL — no schema drop, no base bulk-inserts, no MV refresh.
    Trust signals are already isolated on :class:`TrustBase` and the API does
    a query-time JOIN against them, so neither step is needed when iterating
    on KGC's trust stage outputs alone.
    """
    kg_dir = parquet_dir.resolve()
    logger.info("Loading trust signals only from %s", kg_dir)
    _load_trust_signals(conn, kg_dir)


def refresh_materialized_views(conn: Connection) -> None:
    """Rebuild all materialized views from existing base tables."""
    logger.info("Materializing entity views + composition tables...")
    refresh_all(conn)
    logger.info("Materializing search + statistics...")
    refresh_search(conn)

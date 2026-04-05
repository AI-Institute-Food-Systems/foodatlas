"""Initial schema: base tables and materialized API tables.

Revision ID: 001
Revises: None
Create Date: 2026-04-04

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # --- Base tables (normalized, from KGC parquet) ---

    op.create_table(
        "base_entities",
        sa.Column("foodatlas_id", sa.String(20), primary_key=True),
        sa.Column("entity_type", sa.String(20), nullable=False),
        sa.Column("common_name", sa.Text, nullable=False),
        sa.Column("scientific_name", sa.Text, server_default=""),
        sa.Column("synonyms", postgresql.ARRAY(sa.Text), server_default="{}"),
        sa.Column("external_ids", postgresql.JSONB, server_default="{}"),
    )
    op.create_index("ix_base_entities_type", "base_entities", ["entity_type"])
    op.create_index("ix_base_entities_common_name", "base_entities", ["common_name"])
    op.create_index(
        "ix_base_entities_type_name", "base_entities", ["entity_type", "common_name"]
    )

    op.create_table(
        "relationships",
        sa.Column("foodatlas_id", sa.String(10), primary_key=True),
        sa.Column("name", sa.Text, nullable=False, unique=True),
    )

    op.create_table(
        "base_triplets",
        sa.Column("triplet_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "head_id",
            sa.String(20),
            sa.ForeignKey("base_entities.foodatlas_id"),
            nullable=False,
        ),
        sa.Column(
            "relationship_id",
            sa.String(10),
            sa.ForeignKey("relationships.foodatlas_id"),
            nullable=False,
        ),
        sa.Column(
            "tail_id",
            sa.String(20),
            sa.ForeignKey("base_entities.foodatlas_id"),
            nullable=False,
        ),
        sa.Column("source", sa.Text, server_default=""),
        sa.Column("attestation_ids", postgresql.ARRAY(sa.Text), server_default="{}"),
        sa.UniqueConstraint("head_id", "relationship_id", "tail_id", name="uq_triplet"),
    )
    op.create_index("ix_base_triplets_head", "base_triplets", ["head_id"])
    op.create_index("ix_base_triplets_tail", "base_triplets", ["tail_id"])
    op.create_index("ix_base_triplets_rel", "base_triplets", ["relationship_id"])
    op.create_index(
        "ix_base_triplets_head_rel",
        "base_triplets",
        ["head_id", "relationship_id"],
    )
    op.create_index(
        "ix_base_triplets_tail_rel",
        "base_triplets",
        ["tail_id", "relationship_id"],
    )

    op.create_table(
        "base_evidence",
        sa.Column("evidence_id", sa.String(30), primary_key=True),
        sa.Column("source_type", sa.Text, nullable=False),
        sa.Column("reference", postgresql.JSONB, nullable=False),
    )
    op.create_index("ix_base_evidence_source_type", "base_evidence", ["source_type"])

    op.create_table(
        "base_attestations",
        sa.Column("attestation_id", sa.String(30), primary_key=True),
        sa.Column(
            "evidence_id",
            sa.String(30),
            sa.ForeignKey("base_evidence.evidence_id"),
            nullable=False,
        ),
        sa.Column("source", sa.Text, nullable=False),
        sa.Column("head_name_raw", sa.Text, server_default=""),
        sa.Column("tail_name_raw", sa.Text, server_default=""),
        sa.Column("conc_value", sa.Double, nullable=True),
        sa.Column("conc_unit", sa.Text, server_default=""),
        sa.Column("conc_value_raw", sa.Text, server_default=""),
        sa.Column("conc_unit_raw", sa.Text, server_default=""),
        sa.Column("food_part", sa.Text, server_default=""),
        sa.Column("food_processing", sa.Text, server_default=""),
        sa.Column("quality_score", sa.Double, nullable=True),
        sa.Column("validated", sa.Boolean, server_default="false"),
        sa.Column("validated_correct", sa.Boolean, server_default="true"),
        sa.Column("head_candidates", postgresql.ARRAY(sa.Text), server_default="{}"),
        sa.Column("tail_candidates", postgresql.ARRAY(sa.Text), server_default="{}"),
    )
    op.create_index(
        "ix_base_attestations_evidence", "base_attestations", ["evidence_id"]
    )
    op.create_index("ix_base_attestations_source", "base_attestations", ["source"])

    # --- Materialized API tables ---

    op.create_table(
        "mv_food_entities",
        sa.Column("foodatlas_id", sa.String(20), primary_key=True),
        sa.Column("entity_type", sa.String(20), nullable=False),
        sa.Column("common_name", sa.Text, nullable=False),
        sa.Column("scientific_name", sa.Text, server_default=""),
        sa.Column("synonyms", postgresql.ARRAY(sa.Text), server_default="{}"),
        sa.Column("external_ids", postgresql.JSONB, server_default="{}"),
        sa.Column(
            "food_classification", postgresql.ARRAY(sa.Text), server_default="{}"
        ),
    )
    op.create_index("ix_mv_food_common_name", "mv_food_entities", ["common_name"])

    op.create_table(
        "mv_chemical_entities",
        sa.Column("foodatlas_id", sa.String(20), primary_key=True),
        sa.Column("entity_type", sa.String(20), nullable=False),
        sa.Column("common_name", sa.Text, nullable=False),
        sa.Column("scientific_name", sa.Text, server_default=""),
        sa.Column("synonyms", postgresql.ARRAY(sa.Text), server_default="{}"),
        sa.Column("external_ids", postgresql.JSONB, server_default="{}"),
        sa.Column(
            "chemical_classification",
            postgresql.ARRAY(sa.Text),
            server_default="{}",
        ),
        sa.Column(
            "nutrient_classification",
            postgresql.ARRAY(sa.Text),
            server_default="{}",
        ),
    )
    op.create_index("ix_mv_chem_common_name", "mv_chemical_entities", ["common_name"])

    op.create_table(
        "mv_disease_entities",
        sa.Column("foodatlas_id", sa.String(20), primary_key=True),
        sa.Column("entity_type", sa.String(20), nullable=False),
        sa.Column("common_name", sa.Text, nullable=False),
        sa.Column("scientific_name", sa.Text, server_default=""),
        sa.Column("synonyms", postgresql.ARRAY(sa.Text), server_default="{}"),
        sa.Column("external_ids", postgresql.JSONB, server_default="{}"),
    )
    op.create_index("ix_mv_disease_common_name", "mv_disease_entities", ["common_name"])

    op.create_table(
        "mv_food_chemical_composition",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("food_name", sa.Text, nullable=False),
        sa.Column("food_foodatlas_id", sa.String(20), nullable=False),
        sa.Column("chemical_name", sa.Text, nullable=False),
        sa.Column("chemical_foodatlas_id", sa.String(20), nullable=False),
        sa.Column(
            "nutrient_classification",
            postgresql.ARRAY(sa.Text),
            server_default="{}",
        ),
        sa.Column("median_concentration", postgresql.JSONB, nullable=True),
        sa.Column("fdc_evidences", postgresql.JSONB, nullable=True),
        sa.Column("foodatlas_evidences", postgresql.JSONB, nullable=True),
        sa.Column("dmd_evidences", postgresql.JSONB, nullable=True),
    )
    op.create_index(
        "ix_mv_fcc_food_name", "mv_food_chemical_composition", ["food_name"]
    )
    op.create_index(
        "ix_mv_fcc_chemical_name",
        "mv_food_chemical_composition",
        ["chemical_name"],
    )

    op.create_table(
        "mv_chemical_disease_correlation",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("chemical_name", sa.Text, nullable=False),
        sa.Column("chemical_foodatlas_id", sa.String(20), nullable=False),
        sa.Column("relationship_id", sa.String(10), nullable=False),
        sa.Column("disease_name", sa.Text, nullable=False),
        sa.Column("disease_foodatlas_id", sa.String(20), nullable=False),
        sa.Column("sources", postgresql.ARRAY(sa.Text), server_default="{}"),
        sa.Column("evidences", postgresql.JSONB, server_default="[]"),
    )
    op.create_index(
        "ix_mv_cdc_chem_rel",
        "mv_chemical_disease_correlation",
        ["chemical_name", "relationship_id"],
    )
    op.create_index(
        "ix_mv_cdc_disease_rel",
        "mv_chemical_disease_correlation",
        ["disease_name", "relationship_id"],
    )

    op.create_table(
        "mv_search_auto_complete",
        sa.Column("foodatlas_id", sa.String(20), primary_key=True),
        sa.Column("associations", sa.Integer, server_default="0"),
        sa.Column("entity_type", sa.String(20), nullable=False),
        sa.Column("common_name", sa.Text, nullable=False),
        sa.Column("scientific_name", sa.Text, server_default=""),
        sa.Column("synonyms", postgresql.ARRAY(sa.Text), server_default="{}"),
        sa.Column("external_ids", postgresql.JSONB, server_default="{}"),
        sa.Column("exact_auto", postgresql.ARRAY(sa.Text), server_default="{}"),
        sa.Column("substr_auto", sa.Text, server_default=""),
    )
    op.create_index(
        "ix_mv_search_substr_trgm",
        "mv_search_auto_complete",
        ["substr_auto"],
        postgresql_using="gin",
        postgresql_ops={"substr_auto": "gin_trgm_ops"},
    )
    op.create_index(
        "ix_mv_search_exact",
        "mv_search_auto_complete",
        ["exact_auto"],
        postgresql_using="gin",
    )

    op.create_table(
        "mv_metadata_statistics",
        sa.Column("field", sa.Text, primary_key=True),
        sa.Column("count", sa.BigInteger, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("mv_metadata_statistics")
    op.drop_table("mv_search_auto_complete")
    op.drop_table("mv_chemical_disease_correlation")
    op.drop_table("mv_food_chemical_composition")
    op.drop_table("mv_disease_entities")
    op.drop_table("mv_chemical_entities")
    op.drop_table("mv_food_entities")
    op.drop_table("base_attestations")
    op.drop_table("base_evidence")
    op.drop_table("base_triplets")
    op.drop_table("relationships")
    op.drop_table("base_entities")
    op.execute("DROP EXTENSION IF EXISTS pg_trgm")

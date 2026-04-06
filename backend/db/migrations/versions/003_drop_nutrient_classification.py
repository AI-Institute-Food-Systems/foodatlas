"""Drop nutrient_classification from chemical entities and composition.

chemical_groups from entity attributes now populates chemical_classification
only. nutrient_classification was redundant.

Revision ID: 003
Revises: 002
Create Date: 2026-04-06

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "003"
down_revision: str = "002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("mv_chemical_entities", "nutrient_classification")
    op.drop_column("mv_food_chemical_composition", "nutrient_classification")


def downgrade() -> None:
    op.add_column(
        "mv_chemical_entities",
        sa.Column(
            "nutrient_classification",
            postgresql.ARRAY(sa.Text),
            server_default="{}",
        ),
    )
    op.add_column(
        "mv_food_chemical_composition",
        sa.Column(
            "nutrient_classification",
            postgresql.ARRAY(sa.Text),
            server_default="{}",
        ),
    )

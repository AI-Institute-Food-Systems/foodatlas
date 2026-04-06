"""Add attributes JSONB column to base_entities.

Revision ID: 002
Revises: 001
Create Date: 2026-04-06

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "002"
down_revision: str = "001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "base_entities",
        sa.Column("attributes", postgresql.JSONB, server_default="{}"),
    )


def downgrade() -> None:
    op.drop_column("base_entities", "attributes")

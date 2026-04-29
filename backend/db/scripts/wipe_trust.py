"""Wipe all rows from base_trust_signals (keeps the table).

Use when iterating on the trust prompt before shipping — re-judging produces
new ``signal_id``s with different ``config_hash``, so old rows accumulate
unless deleted. The API now picks the latest score per attestation, so this
isn't strictly required, but it's nice for a clean snapshot.

Usage::

    cd backend/db
    uv run python scripts/wipe_trust.py
"""

from __future__ import annotations

from sqlalchemy import text
from src.config import DBSettings
from src.engine import create_sync_engine


def main() -> None:
    settings = DBSettings()
    engine = create_sync_engine(settings)
    with engine.connect() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM base_trust_signals")).scalar()
        conn.execute(text("DELETE FROM base_trust_signals"))
        conn.commit()
        after = conn.execute(text("SELECT COUNT(*) FROM base_trust_signals")).scalar()
    print(f"Deleted {before} rows, table now has {after} rows.")


if __name__ == "__main__":
    main()

"""Ad-hoc diagnostic: dump trust scores for a (food, chemical) pair.

Filters by **canonical** food and chemical names — matching the values
shown in the UI (`mv_food_chemical_composition.food_name` /
`chemical_name`) — not by the raw IE-extracted strings. So
`inspect_trust.py "tomato (raw)" "linoleic acid"` returns every
attestation that resolves to those entities, regardless of whether IE
extracted "tomato", "tomato fruit", "Tomato, raw", etc. The raw names
remain in the output for inspection.

Usage::

    cd backend/db
    uv run python scripts/inspect_trust.py "tomato (raw)" "linoleic acid"
    uv run python scripts/inspect_trust.py tomato carbohydrate
"""

from __future__ import annotations

import sys

from sqlalchemy import text
from src.config import DBSettings
from src.engine import create_sync_engine


def main(food: str, chemical: str) -> None:
    settings = DBSettings()
    engine = create_sync_engine(settings)

    sql = text("""
        WITH food AS (
            SELECT foodatlas_id FROM base_entities WHERE common_name = :food
        ),
        chem AS (
            SELECT foodatlas_id FROM base_entities WHERE common_name = :chemical
        ),
        atts AS (
            SELECT DISTINCT unnest(bt.attestation_ids) AS attestation_id
            FROM base_triplets bt, food, chem
            WHERE bt.head_id = food.foodatlas_id
              AND bt.tail_id = chem.foodatlas_id
              AND bt.relationship_id = 'r1'
        )
        SELECT
            a.attestation_id,
            a.source,
            a.head_name_raw,
            a.tail_name_raw,
            a.conc_value,
            a.conc_value_raw || ' ' || a.conc_unit_raw AS original,
            ts.score,
            ts.reason
        FROM atts
        JOIN base_attestations a USING (attestation_id)
        LEFT JOIN base_trust_signals ts
               ON ts.attestation_id = a.attestation_id
              AND ts.signal_kind = 'llm_plausibility'
              AND ts.score >= 0
        ORDER BY ts.score NULLS LAST, a.attestation_id
    """)

    with engine.connect() as conn:
        rows = list(conn.execute(sql, {"food": food, "chemical": chemical}))

    if not rows:
        print(
            f"No attestations found for canonical (food={food!r}, "
            f"chemical={chemical!r}). Check the names against the UI."
        )
        return

    print(f"{len(rows)} attestations for ({food!r}, {chemical!r}):")
    print()
    for r in rows:
        m = r._mapping
        score_str = f"{m['score']:.3f}" if m["score"] is not None else "(no signal)"
        reason = (m["reason"] or "")[:80]
        raw_food = (m["head_name_raw"] or "")[:18]
        raw_chem = (m["tail_name_raw"] or "")[:20]
        print(
            f"  {m['attestation_id']:<16} "
            f"src={m['source']:<25} "
            f"raw=({raw_food} / {raw_chem}) "
            f"orig={(m['original'] or '').strip():<22} "
            f"score={score_str:<14} "
            f"reason={reason}"
        )


if __name__ == "__main__":
    food_arg = sys.argv[1] if len(sys.argv) > 1 else "tomato (raw)"
    chem_arg = sys.argv[2] if len(sys.argv) > 2 else "linoleic acid"
    main(food_arg, chem_arg)

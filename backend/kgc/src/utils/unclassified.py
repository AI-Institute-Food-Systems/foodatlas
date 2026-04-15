"""Unclassified entity detection: foods/chemicals with no IS_A parent."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path

_IS_A_RELATIONSHIP = "r2"


def find_unclassified(ents: pd.DataFrame, trips: pd.DataFrame) -> pd.DataFrame:
    """Return food/chemical entities with no IS_A parent.

    The two ontologies use opposite IS_A direction conventions:

    - ChEBI (chemicals): ``head=parent, tail=child`` — a chemical has a
      parent iff it appears as a *tail*.
    - FoodOn (foods): ``head=child, tail=parent`` — a food has a parent
      iff it appears as a *head*.
    """
    is_a = trips[trips["relationship_id"] == _IS_A_RELATIONSHIP]
    chem_classified = set(is_a["tail_id"])
    food_classified = set(is_a["head_id"])

    chems = ents[ents["entity_type"] == "chemical"]
    foods = ents[ents["entity_type"] == "food"]

    unclassified_chems = chems[~chems.index.isin(chem_classified)]
    unclassified_foods = foods[~foods.index.isin(food_classified)]
    return pd.concat([unclassified_chems, unclassified_foods])


def _attestation_counts(trips: pd.DataFrame) -> dict[str, int]:
    """Sum attestation_ids length per entity across all triplets it touches.

    An entity's attestation count is the total number of attestations on
    every triplet it appears in (as head or tail). Ontology IS_A triplets
    have empty attestation_ids and contribute 0.
    """
    if "attestation_ids" not in trips.columns:
        return {}
    counts: dict[str, int] = {}
    for _, row in trips.iterrows():
        atts = row["attestation_ids"] or []
        n = len(atts)
        if n == 0:
            continue
        counts[row["head_id"]] = counts.get(row["head_id"], 0) + n
        counts[row["tail_id"]] = counts.get(row["tail_id"], 0) + n
    return counts


def write_unclassified_jsonl(
    ents: pd.DataFrame,
    trips: pd.DataFrame,
    out_path: Path,
) -> int:
    """Write unclassified entities to *out_path* as JSONL, sorted by
    attestation count (descending). Returns count written.
    """
    unclassified = find_unclassified(ents, trips)
    att_counts = _attestation_counts(trips)

    rows = [
        {
            "foodatlas_id": eid,
            "entity_type": row.get("entity_type", ""),
            "common_name": row.get("common_name", ""),
            "attestation_count": att_counts.get(eid, 0),
        }
        for eid, row in unclassified.iterrows()
    ]
    rows.sort(key=lambda r: r["attestation_count"], reverse=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for record in rows:
            f.write(json.dumps(record) + "\n")
    return len(rows)

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


def write_unclassified_jsonl(
    ents: pd.DataFrame,
    trips: pd.DataFrame,
    out_path: Path,
) -> int:
    """Write unclassified entities to *out_path* as JSONL. Returns count."""
    unclassified = find_unclassified(ents, trips)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for eid, row in unclassified.iterrows():
            record = {
                "foodatlas_id": eid,
                "entity_type": row.get("entity_type", ""),
                "common_name": row.get("common_name", ""),
            }
            f.write(json.dumps(record) + "\n")
    return len(unclassified)

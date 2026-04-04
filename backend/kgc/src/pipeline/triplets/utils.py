"""Shared utilities for triplet builders."""

from __future__ import annotations

import pandas as pd


def explode_external_ids(entities: pd.DataFrame, key: str) -> pd.DataFrame:
    """Build a DataFrame mapping native IDs to entity IDs with candidate lists.

    Returns columns: ``native_id``, ``foodatlas_id``, ``candidates``.
    ``candidates`` is the full list of entity IDs for that native ID.
    """
    rows: list[tuple[str, str]] = []
    for eid, row in entities.iterrows():
        for native_id in row["external_ids"].get(key, []):
            rows.append((str(native_id), str(eid)))
    if not rows:
        return pd.DataFrame(columns=["native_id", "foodatlas_id", "candidates"])

    lookup = pd.DataFrame(rows, columns=["native_id", "foodatlas_id"])
    # Add candidate lists (all entity IDs per native_id).
    candidates = lookup.groupby("native_id")["foodatlas_id"].apply(list)
    candidates.name = "candidates"
    return lookup.merge(candidates, left_on="native_id", right_index=True)

"""Helpers shared by the KG delta-merge scripts (``merge_*_delta.py``)."""

from __future__ import annotations

import ast
import json


def _parse_list(cell) -> list[str]:
    """Coerce a parquet/CSV list cell (JSON, repr, ndarray, list) to ``list[str]``."""
    if isinstance(cell, (list, tuple)):
        return [str(x) for x in cell]
    if hasattr(cell, "tolist"):
        return [str(x) for x in cell.tolist()]
    if isinstance(cell, str) and cell.strip():
        try:
            v = ast.literal_eval(cell)
        except (ValueError, SyntaxError):
            try:
                v = json.loads(cell)
            except json.JSONDecodeError:
                v = [cell]
        return [str(x) for x in (v if isinstance(v, (list, tuple)) else [v])]
    return []


def _max_id(series) -> int:
    """Largest numeric suffix among ``e<N>``-style ids in ``series``."""
    return max(int(s[1:]) for s in series if isinstance(s, str) and s[1:].isdigit())

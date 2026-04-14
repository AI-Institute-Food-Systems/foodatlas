"""Write IE diagnostics: unresolved names."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ...stores.schema import FILE_IE_UNRESOLVED

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


def _build_summary(
    metadata: pd.DataFrame, col: str, names: set[str]
) -> dict[str, tuple[int, list[str]]]:
    """Pre-aggregate occurrences and sample references in one pass."""
    subset = metadata.loc[metadata[col].isin(names), [col, "reference"]]
    grouped = subset.groupby(col)["reference"]
    counts = grouped.size()
    samples = grouped.apply(lambda g: g.head(3).tolist())
    return {name: (int(counts.get(name, 0)), samples.get(name, [])) for name in names}


def _normalize_refs(refs: list) -> list[str]:
    return [r[0] if isinstance(r, list) and r else str(r) for r in refs]


def write_unresolved_report(
    unresolved_food: set[str],
    unresolved_chemical: set[str],
    metadata: pd.DataFrame,
    output_dir: Path,
) -> Path:
    """Append unresolved names to ``ie_unresolved.jsonl``."""
    out = Path(output_dir) / FILE_IE_UNRESOLVED
    out.parent.mkdir(exist_ok=True)

    food_summary = _build_summary(metadata, "_food_name", unresolved_food)
    chem_summary = _build_summary(metadata, "_chemical_name", unresolved_chemical)

    count = 0
    with out.open("a") as f:
        for name in unresolved_food:
            occ, refs = food_summary[name]
            line = json.dumps(
                {
                    "name": name,
                    "entity_type": "food",
                    "occurrences": occ,
                    "sample_references": _normalize_refs(refs),
                },
                ensure_ascii=False,
            )
            f.write(line + "\n")
            count += 1

        for name in unresolved_chemical:
            occ, refs = chem_summary[name]
            line = json.dumps(
                {
                    "name": name,
                    "entity_type": "chemical",
                    "occurrences": occ,
                    "sample_references": _normalize_refs(refs),
                },
                ensure_ascii=False,
            )
            f.write(line + "\n")
            count += 1

    logger.info("Appended %d unresolved names to %s.", count, out)
    return out

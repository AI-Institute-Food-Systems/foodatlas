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


def write_unresolved_report(
    unresolved_food: set[str],
    unresolved_chemical: set[str],
    metadata: pd.DataFrame,
    output_dir: Path,
) -> Path:
    """Append unresolved names to ``ie_unresolved.jsonl``."""
    out = Path(output_dir) / FILE_IE_UNRESOLVED
    out.parent.mkdir(exist_ok=True)

    count = 0
    with out.open("a") as f:
        for name in unresolved_food:
            occ = int((metadata["_food_name"] == name).sum())
            refs = (
                metadata.loc[metadata["_food_name"] == name, "reference"]
                .head(3)
                .tolist()
            )
            line = json.dumps(
                {
                    "name": name,
                    "entity_type": "food",
                    "occurrences": occ,
                    "sample_references": [
                        r[0] if isinstance(r, list) and r else str(r) for r in refs
                    ],
                },
                ensure_ascii=False,
            )
            f.write(line + "\n")
            count += 1

        for name in unresolved_chemical:
            occ = int((metadata["_chemical_name"] == name).sum())
            refs = (
                metadata.loc[metadata["_chemical_name"] == name, "reference"]
                .head(3)
                .tolist()
            )
            line = json.dumps(
                {
                    "name": name,
                    "entity_type": "chemical",
                    "occurrences": occ,
                    "sample_references": [
                        r[0] if isinstance(r, list) and r else str(r) for r in refs
                    ],
                },
                ensure_ascii=False,
            )
            f.write(line + "\n")
            count += 1

    logger.info("Appended %d unresolved names to %s.", count, out)
    return out

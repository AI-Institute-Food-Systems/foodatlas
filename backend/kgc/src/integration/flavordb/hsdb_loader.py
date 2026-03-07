"""HSDB (Hazardous Substances Data Bank) flavor data loader."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def load_hsdb(
    hsdb_dir: Path,
) -> tuple[dict[int, list[dict]], dict[int, list[dict]]]:
    """Load HSDB odor and taste annotations from JSON files.

    Args:
        hsdb_dir: Directory containing HSDB annotation JSON files.

    Returns:
        Tuple of (cid_to_odor, cid_to_taste) mappings from PubChem CID
        to lists of ``{"value": str, "hsdb_url": str}`` dicts.

    Raises:
        ValueError: If a StringWithMarkup entry has unexpected length.
    """
    odor_path = next(hsdb_dir.glob("*Odor*.json"))
    taste_path = next(hsdb_dir.glob("*Taste*.json"))

    with odor_path.open() as f:
        hsdb_odor = json.load(f)
    with taste_path.open() as f:
        hsdb_taste = json.load(f)

    cid2odor: dict[int, list[dict]] = {}
    cid2taste: dict[int, list[dict]] = {}
    _map_cid_to_hsdb_flavor(hsdb_odor, cid2odor)
    _map_cid_to_hsdb_flavor(hsdb_taste, cid2taste)
    return cid2odor, cid2taste


def _map_cid_to_hsdb_flavor(
    hsdb: dict,
    mapper: dict[int, list[dict]],
) -> None:
    """Populate a CID→flavor mapping from an HSDB annotation dict."""
    for annot in hsdb["Annotations"]["Annotation"]:
        if "LinkedRecords" not in annot:
            continue
        for cid in annot["LinkedRecords"]["CID"]:
            if cid not in mapper:
                mapper[cid] = []
            for data in annot["Data"]:
                value = data["Value"]["StringWithMarkup"]
                if len(value) != 1:
                    msg = f"Expected 1 StringWithMarkup entry, got {len(value)}"
                    raise ValueError(msg)
                mapper[cid].append(
                    {
                        "value": value[0]["String"],
                        "hsdb_url": annot["URL"],
                    }
                )

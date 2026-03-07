"""Flavor metadata generation from FlavorDB and HSDB data."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd
from thefuzz import process as fuzz_process

if TYPE_CHECKING:
    from pathlib import Path

from .hsdb_loader import load_hsdb

logger = logging.getLogger(__name__)


def load_flavordb_data(data_path: Path) -> dict:
    """Load FlavorDB scraped data from a JSON file.

    Args:
        data_path: Path to the FlavorDB JSON file.

    Returns:
        Dict mapping PubChem CID to chemical flavor data.
    """
    with data_path.open() as f:
        result: dict = json.load(f)
    return result


def extract_flavordb_metadata(
    flavordb_data: dict,
    entity_pc_ids: set[int],
    entity_pc_id_to_name: dict[int, str],
) -> pd.DataFrame:
    """Extract flavor metadata entries from FlavorDB data.

    Args:
        flavordb_data: Dict from ``load_flavordb_data``.
        entity_pc_ids: Set of PubChem CIDs present in entities.
        entity_pc_id_to_name: PubChem CID -> common_name mapping.

    Returns:
        DataFrame with flavor metadata rows.
    """
    rows: list[dict] = []
    for pc_id_str, chemical in flavordb_data.items():
        pc_id = int(pc_id_str)
        if pc_id not in entity_pc_ids:
            continue

        descriptors: set[str] = set()
        for field in [
            "flavor_profile",
            "taste",
            "odor",
            "fooddb_flavor_profile",
            "super_sweet",
        ]:
            val = chemical.get(field, "")
            if val:
                descriptors.update(f.lower() for f in val.split("@") if f)
        if chemical.get("bitter"):
            descriptors.add("bitter")

        for flavor in descriptors:
            rows.append(
                {
                    "foodatlas_id": "mf",
                    "source": "flavordb",
                    "reference": {
                        "url": (
                            "https://cosylab.iiitd.edu.in/flavordb/"
                            f"molecules_json?id={pc_id}"
                        ),
                    },
                    "_flavor": flavor,
                    "_chemical": entity_pc_id_to_name[pc_id],
                    "_pubchem_id": pc_id,
                }
            )
    return pd.DataFrame(rows)


def extract_hsdb_metadata(
    cid2odor: dict[int, list[dict]],
    cid2taste: dict[int, list[dict]],
    skip_pc_ids: set[int],
    entity_pc_id_to_name: dict[int, str],
) -> pd.DataFrame:
    """Extract flavor metadata from HSDB (PubChem annotations).

    Skips CIDs already covered by FlavorDB to avoid duplication.

    Args:
        cid2odor: CID->odor mapping from HSDB.
        cid2taste: CID->taste mapping from HSDB.
        skip_pc_ids: CIDs to skip (already in FlavorDB data).
        entity_pc_id_to_name: CID->common_name mapping for entities.

    Returns:
        DataFrame with HSDB flavor metadata rows.
    """
    rows: list[dict] = []
    for mapper in [cid2odor, cid2taste]:
        for pc_id, data in mapper.items():
            if pc_id in skip_pc_ids or pc_id not in entity_pc_id_to_name:
                continue
            for ref in data:
                rows.append(
                    {
                        "foodatlas_id": "mf",
                        "_pubchem_id": pc_id,
                        "source": "hsdb",
                        "reference": {"url": ref["hsdb_url"]},
                        "_flavor": ref["value"],
                        "_chemical": entity_pc_id_to_name[pc_id],
                    }
                )
    return pd.DataFrame(rows)


def fuzzy_match_flavors(
    pubchem_flavor: pd.DataFrame,
    reference_flavors: pd.Series,
    threshold: int = 90,
) -> pd.DataFrame:
    """Filter HSDB flavors by fuzzy matching against FlavorDB descriptors.

    Args:
        pubchem_flavor: DataFrame with ``_flavor`` column from HSDB.
        reference_flavors: Series of reference flavor descriptors.
        threshold: Minimum fuzzy match score (0-100).

    Returns:
        Filtered DataFrame with ``_flavor`` replaced by matched descriptor.
    """
    if pubchem_flavor.empty or reference_flavors.empty:
        return pubchem_flavor

    matches = pubchem_flavor["_flavor"].apply(
        lambda x: fuzz_process.extract(x, reference_flavors, limit=1)
    )
    close = matches.apply(lambda x: x[0][1] >= threshold if x else False)
    result = pubchem_flavor[close].copy()
    result["_flavor"] = matches[close].apply(lambda x: x[0][0])
    return result.reset_index(drop=True)


def build_flavor_metadata(
    flavordb_data_path: Path,
    hsdb_dir: Path,
    entities: pd.DataFrame,
) -> pd.DataFrame:
    """Build combined flavor metadata from FlavorDB and HSDB.

    Args:
        flavordb_data_path: Path to FlavorDB JSON file.
        hsdb_dir: Directory containing HSDB JSON files.
        entities: Entities DataFrame with ``external_ids`` containing
            ``pubchem_compound`` CIDs.

    Returns:
        DataFrame with deduplicated flavor metadata rows.
    """
    chemicals = entities[entities["entity_type"] == "chemical"].copy()
    chemicals["pc_id"] = chemicals["external_ids"].apply(
        lambda x: int(x["pubchem_compound"][0]) if "pubchem_compound" in x else None
    )
    chemicals = chemicals.dropna(subset=["pc_id"])
    chemicals["pc_id"] = chemicals["pc_id"].astype(int)

    entity_pc_ids = set(chemicals["pc_id"])
    pc_id_to_name = dict(
        zip(chemicals["pc_id"], chemicals["common_name"], strict=False)
    )

    flavordb_data = load_flavordb_data(flavordb_data_path)
    flavordb_meta = extract_flavordb_metadata(
        flavordb_data, entity_pc_ids, pc_id_to_name
    )

    cid2odor, cid2taste = load_hsdb(hsdb_dir)
    skip_ids = set(flavordb_meta["_pubchem_id"]) if not flavordb_meta.empty else set()
    hsdb_meta = extract_hsdb_metadata(cid2odor, cid2taste, skip_ids, pc_id_to_name)

    if not flavordb_meta.empty and not hsdb_meta.empty:
        ref_flavors = flavordb_meta["_flavor"].drop_duplicates().reset_index(drop=True)
        hsdb_meta = fuzzy_match_flavors(hsdb_meta, ref_flavors)

    combined = pd.concat([flavordb_meta, hsdb_meta], ignore_index=True)
    if combined.empty:
        return combined

    combined["__url"] = combined["reference"].apply(lambda x: x.get("url", ""))
    combined = combined.drop_duplicates(
        subset=["source", "_pubchem_id", "_flavor", "__url"],
    )
    combined = combined.drop(columns=["__url"]).reset_index(drop=True)

    combined["foodatlas_id"] = ["mf" + str(i + 1) for i in range(len(combined))]
    return combined

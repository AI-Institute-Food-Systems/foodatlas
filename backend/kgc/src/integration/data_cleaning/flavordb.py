"""Clean raw FlavorDB and HSDB data into a single parquet file."""

import json
import logging
from pathlib import Path

import pandas as pd
from thefuzz import process as fuzz_process

from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


def process_flavordb(settings: KGCSettings) -> None:
    """Parse FlavorDB JSON + HSDB JSON, merge, and save cleaned parquet."""
    data_dir = Path(settings.data_dir)
    dp_dir = Path(settings.data_cleaning_dir)
    dp_dir.mkdir(parents=True, exist_ok=True)

    flavordb_path = data_dir / "FlavorDB" / "flavordb_scrape.json"
    hsdb_dir = data_dir / "HSDB"

    flavordb_data = _load_flavordb_json(flavordb_path)
    flavordb_meta = _extract_flavordb_metadata(flavordb_data)

    cid2odor, cid2taste = _load_hsdb(hsdb_dir)
    skip_ids = set(flavordb_meta["_pubchem_id"]) if not flavordb_meta.empty else set()
    hsdb_meta = _extract_hsdb_metadata(cid2odor, cid2taste, skip_ids)

    if not flavordb_meta.empty and not hsdb_meta.empty:
        ref_flavors = flavordb_meta["_flavor"].drop_duplicates().reset_index(drop=True)
        hsdb_meta = _fuzzy_match_flavors(hsdb_meta, ref_flavors)

    combined = pd.concat([flavordb_meta, hsdb_meta], ignore_index=True)
    if not combined.empty:
        combined["__url"] = combined["reference"].apply(lambda x: x.get("url", ""))
        combined = combined.drop_duplicates(
            subset=["source", "_pubchem_id", "_flavor", "__url"],
        )
        combined = combined.drop(columns=["__url"]).reset_index(drop=True)
        combined["foodatlas_id"] = [f"mf{i + 1}" for i in range(len(combined))]

    combined.to_parquet(dp_dir / "flavor_metadata_cleaned.parquet")
    logger.info("Processed FlavorDB/HSDB: %d metadata rows.", len(combined))


def _load_flavordb_json(data_path: Path) -> dict:
    with data_path.open() as f:
        result: dict = json.load(f)
    return result


def _extract_flavordb_metadata(flavordb_data: dict) -> pd.DataFrame:
    """Extract ALL flavor descriptors from FlavorDB (no entity filtering)."""
    rows: list[dict] = []
    for pc_id_str, chemical in flavordb_data.items():
        pc_id = int(pc_id_str)
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
                    "_pubchem_id": pc_id,
                }
            )
    return pd.DataFrame(rows)


def _load_hsdb(
    hsdb_dir: Path,
) -> tuple[dict[int, list[dict]], dict[int, list[dict]]]:
    """Load HSDB odor and taste annotations from JSON files."""
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


def _extract_hsdb_metadata(
    cid2odor: dict[int, list[dict]],
    cid2taste: dict[int, list[dict]],
    skip_pc_ids: set[int],
) -> pd.DataFrame:
    """Extract flavor metadata from HSDB (no entity filtering)."""
    rows: list[dict] = []
    for mapper in [cid2odor, cid2taste]:
        for pc_id, data in mapper.items():
            if pc_id in skip_pc_ids:
                continue
            for ref in data:
                rows.append(
                    {
                        "foodatlas_id": "mf",
                        "_pubchem_id": pc_id,
                        "source": "hsdb",
                        "reference": {"url": ref["hsdb_url"]},
                        "_flavor": ref["value"],
                    }
                )
    return pd.DataFrame(rows)


def _fuzzy_match_flavors(
    pubchem_flavor: pd.DataFrame,
    reference_flavors: pd.Series,
    threshold: int = 90,
) -> pd.DataFrame:
    if pubchem_flavor.empty or reference_flavors.empty:
        return pubchem_flavor

    matches = pubchem_flavor["_flavor"].apply(
        lambda x: fuzz_process.extract(x, reference_flavors, limit=1)
    )
    close = matches.apply(lambda x: x[0][1] >= threshold if x else False)
    result = pubchem_flavor[close].copy()
    result["_flavor"] = matches[close].apply(lambda x: x[0][0])
    return result.reset_index(drop=True)

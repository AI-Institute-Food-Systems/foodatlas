"""Clean raw FlavorDB and HSDB data into a single parquet file."""

import logging
from pathlib import Path

import pandas as pd
from thefuzz import process as fuzz_process

from ...models.settings import KGCSettings
from ...utils.json_io import read_json

logger = logging.getLogger(__name__)


def process_flavordb(settings: KGCSettings) -> None:
    """Parse FlavorDB JSON + HSDB JSON, merge, and save cleaned parquet.

    Output columns: ``_pubchem_id``, ``_flavor``, ``_source``, ``_url``.
    """
    data_dir = Path(settings.data_dir)
    dp_dir = Path(settings.data_cleaning_dir)
    dp_dir.mkdir(parents=True, exist_ok=True)

    flavordb_path = data_dir / "FlavorDB" / "flavordb_scrape.json"
    hsdb_dir = data_dir / "HSDB"

    flavordb_data = _load_flavordb_json(flavordb_path)
    flavordb_rows = _extract_flavordb_rows(flavordb_data)

    cid2odor, cid2taste = _load_hsdb(hsdb_dir)
    skip_ids = set(flavordb_rows["_pubchem_id"]) if not flavordb_rows.empty else set()
    hsdb_rows = _extract_hsdb_rows(cid2odor, cid2taste, skip_ids)

    if not flavordb_rows.empty and not hsdb_rows.empty:
        ref_flavors = flavordb_rows["_flavor"].drop_duplicates().reset_index(drop=True)
        hsdb_rows = _fuzzy_match_flavors(hsdb_rows, ref_flavors)

    combined = pd.concat([flavordb_rows, hsdb_rows], ignore_index=True)
    if not combined.empty:
        combined = combined.drop_duplicates(
            subset=["_source", "_pubchem_id", "_flavor", "_url"],
        ).reset_index(drop=True)

    combined.to_parquet(dp_dir / "flavor_cleaned.parquet")
    logger.info("Processed FlavorDB/HSDB: %d rows.", len(combined))


def _load_flavordb_json(data_path: Path) -> dict:
    result: dict = read_json(data_path)
    return result


def _extract_flavordb_rows(flavordb_data: dict) -> pd.DataFrame:
    """Extract (pubchem_id, flavor, source, url) from FlavorDB."""
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

        url = f"https://cosylab.iiitd.edu.in/flavordb/molecules_json?id={pc_id}"
        for flavor in descriptors:
            rows.append(
                {
                    "_pubchem_id": pc_id,
                    "_flavor": flavor,
                    "_source": "flavordb",
                    "_url": url,
                }
            )
    return pd.DataFrame(rows)


def _load_hsdb(
    hsdb_dir: Path,
) -> tuple[dict[int, list[dict]], dict[int, list[dict]]]:
    """Load HSDB odor and taste annotations from JSON files."""
    odor_path = next(hsdb_dir.glob("*Odor*.json"))
    taste_path = next(hsdb_dir.glob("*Taste*.json"))

    hsdb_odor = read_json(odor_path)
    hsdb_taste = read_json(taste_path)

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


def _extract_hsdb_rows(
    cid2odor: dict[int, list[dict]],
    cid2taste: dict[int, list[dict]],
    skip_pc_ids: set[int],
) -> pd.DataFrame:
    """Extract (pubchem_id, flavor, source, url) from HSDB."""
    rows: list[dict] = []
    for mapper in [cid2odor, cid2taste]:
        for pc_id, data in mapper.items():
            if pc_id in skip_pc_ids:
                continue
            for ref in data:
                rows.append(
                    {
                        "_pubchem_id": pc_id,
                        "_flavor": ref["value"],
                        "_source": "hsdb",
                        "_url": ref["hsdb_url"],
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

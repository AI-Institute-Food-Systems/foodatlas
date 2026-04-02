"""FlavorDB/HSDB adapter — faithful ingest of flavor compound data."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from thefuzz import process as fuzz_process

from ....models.ingest import SourceManifest
from ....utils.json_io import read_json
from ..protocol import serialize_raw_attrs, write_manifest

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "flavordb"


class FlavorDBAdapter:
    """Parse FlavorDB JSON + HSDB JSON into standardized ingest parquet."""

    @property
    def source_id(self) -> str:
        return SOURCE_ID

    def ingest(self, raw_dir: Path, output_dir: Path) -> SourceManifest:
        output_dir.mkdir(parents=True, exist_ok=True)

        flavordb_path = raw_dir / "FlavorDB" / "flavordb_scrape.json"
        hsdb_dir = raw_dir / "HSDB"

        flavordb_data: dict = read_json(flavordb_path)
        fdb_rows = _extract_flavordb_rows(flavordb_data)

        cid2odor, cid2taste = _load_hsdb(hsdb_dir)
        skip_ids = set(fdb_rows["_pubchem_id"]) if not fdb_rows.empty else set()
        hsdb_rows = _extract_hsdb_rows(cid2odor, cid2taste, skip_ids)

        if not fdb_rows.empty and not hsdb_rows.empty:
            ref_flavors = fdb_rows["_flavor"].drop_duplicates().reset_index(drop=True)
            hsdb_rows = _fuzzy_match_flavors(hsdb_rows, ref_flavors)

        combined = pd.concat([fdb_rows, hsdb_rows], ignore_index=True)
        if not combined.empty:
            combined = combined.drop_duplicates(
                subset=["_source", "_pubchem_id", "_flavor", "_url"],
            ).reset_index(drop=True)

        nodes, xrefs = _to_standard_schema(combined)

        nodes = serialize_raw_attrs(nodes)

        nodes_path = output_dir / f"{SOURCE_ID}_nodes.parquet"
        xrefs_path = output_dir / f"{SOURCE_ID}_xrefs.parquet"
        nodes.to_parquet(nodes_path)
        xrefs.to_parquet(xrefs_path)

        manifest = SourceManifest(
            source_id=SOURCE_ID,
            node_count=len(nodes),
            xref_count=len(xrefs),
            raw_dir=str(raw_dir),
            output_files=[str(nodes_path), str(xrefs_path)],
        )
        write_manifest(manifest, output_dir)
        logger.info("FlavorDB ingest: %d nodes, %d xrefs.", len(nodes), len(xrefs))
        return manifest


def _to_standard_schema(
    combined: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if combined.empty:
        return pd.DataFrame(), pd.DataFrame()

    grouped = combined.groupby("_pubchem_id")
    node_rows: list[dict] = []
    xref_rows: list[dict] = []

    for pc_id, group in grouped:
        flavors = group["_flavor"].unique().tolist()
        node_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": str(pc_id),
                "name": "",
                "synonyms": [],
                "synonym_types": [],
                "node_type": "flavor_compound",
                "raw_attrs": {"flavors": flavors},
            }
        )
        xref_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": str(pc_id),
                "target_source": "pubchem_cid",
                "target_id": str(pc_id),
            }
        )

    return pd.DataFrame(node_rows), pd.DataFrame(xref_rows)


def _extract_flavordb_rows(flavordb_data: dict) -> pd.DataFrame:
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
        for flavor in sorted(descriptors):
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
    odor_path = next(hsdb_dir.glob("*Odor*.json"))
    taste_path = next(hsdb_dir.glob("*Taste*.json"))
    cid2odor: dict[int, list[dict]] = {}
    cid2taste: dict[int, list[dict]] = {}
    _map_cid_to_flavor(read_json(odor_path), cid2odor)
    _map_cid_to_flavor(read_json(taste_path), cid2taste)
    return cid2odor, cid2taste


def _map_cid_to_flavor(hsdb: dict, mapper: dict[int, list[dict]]) -> None:
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

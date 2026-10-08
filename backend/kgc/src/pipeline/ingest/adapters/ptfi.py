"""PTFI adapter — foods, chemicals, and relative-abundance measurements.

Reads the external-id-keyed source under ``data/PTFI/`` (produced once by
``scripts/derive_ptfi_source.py``) and emits the three standard ingest
artifacts. Keys are PTFI's own identifiers plus PubChem CIDs, never FoodAtlas
entity ids, so entity resolution does the FoodOn-IRI dedup and id minting
natively on every run.

* ``nodes``  — one food per PTFI food group, one chemical per measured CID
* ``edges``  — one ``contains`` edge per (sample, chemical) measurement, carrying
  the relative abundance the triplets stage turns into an attestation
* ``xrefs``  — food → FoodOn IRI, chemical → PubChem CID, the keys Pass 2 links on

Only entities a measurement touches are emitted. PTFI identifies far more
chemicals than it detects — 736 of its 1,022 new chemicals have no positive
abundance in any sample — and a chemical with no measurement can carry no
triplet, so creating it would add an entity with no evidence behind it. Set
:data:`INCLUDE_UNMEASURED` to keep them.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.ingest import SourceManifest
from ..protocol import (
    EDGES_COLUMNS,
    NODES_COLUMNS,
    XREFS_COLUMNS,
    ProgressCallback,
    _noop_progress,
    serialize_raw_attrs,
    write_manifest,
)

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "ptfi"

# Emit entities with no measurement behind them (see module docstring).
INCLUDE_UNMEASURED = False

_FOOD_PREFIX = "PTFI_FOOD"  # native id namespace for a food group
_CHEM_PREFIX = "PTFI_CHEM"  # native id namespace for a chemical (by CID)


class PTFIAdapter:
    """Parse the derived PTFI source into standardized ingest parquet."""

    @property
    def source_id(self) -> str:
        return SOURCE_ID

    def ingest(
        self,
        raw_dir: Path,
        output_dir: Path,
        progress: ProgressCallback = _noop_progress,
    ) -> SourceManifest:
        output_dir.mkdir(parents=True, exist_ok=True)
        ptfi_dir = raw_dir / "PTFI"

        foods = _read_foods(ptfi_dir / "ptfi_foods.csv")
        chemicals = _read_chemicals(ptfi_dir / "ptfi_chemicals.csv")
        measurements = _read_measurements(ptfi_dir / "ptfi_measurements.csv")
        evidence = json.loads((ptfi_dir / "ptfi_evidence.json").read_text())

        total = len(foods) + len(chemicals) + len(measurements)
        progress(0, total)

        foods, chemicals = _drop_unmeasured(foods, chemicals, measurements)
        progress(len(foods) + len(chemicals), total)

        nodes = _build_nodes(foods, chemicals)
        xrefs = _build_xrefs(foods, chemicals)
        edges = _build_edges(foods, measurements, evidence)
        progress(total, total)

        files = _write_outputs(output_dir, nodes, edges, xrefs)
        manifest = SourceManifest(
            source_id=SOURCE_ID,
            node_count=len(nodes),
            edge_count=len(edges),
            xref_count=len(xrefs),
            raw_dir=str(raw_dir),
            output_files=files,
        )
        write_manifest(manifest, output_dir)
        logger.info(
            "PTFI ingest: %d foods, %d chemicals, %d measurements.",
            len(foods),
            len(chemicals),
            len(edges),
        )
        return manifest


# ---------------------------------------------------------------------- reading
def _read_foods(path: Path) -> pd.DataFrame:
    """PTFI food groups; ``sample_ids`` is the list of GGB ids in each group."""
    foods = pd.read_csv(path).fillna("")
    for column in ("ptfi_sample_ids", "synonyms", "food_groups"):
        foods[column] = foods[column].map(_parse_list)
    # The group's native id is its lowest sample id: stable, and every sample id
    # is registered as an alias, so resolution survives the set changing.
    foods["native_id"] = foods["ptfi_sample_ids"].map(
        lambda ids: f"{_FOOD_PREFIX}:{min(ids)}"
    )
    return foods


def _read_chemicals(path: Path) -> pd.DataFrame:
    """Chemicals PTFI measured, keyed by PubChem CID."""
    chemicals = pd.read_csv(path, dtype={"cid": str}).fillna("")
    chemicals["native_id"] = _CHEM_PREFIX + ":" + chemicals["cid"]
    return chemicals


def _read_measurements(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype={"cid": str}).fillna({"conc_unit": ""})


def _drop_unmeasured(
    foods: pd.DataFrame, chemicals: pd.DataFrame, measurements: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Keep only foods and chemicals a measurement actually references."""
    if INCLUDE_UNMEASURED:
        return foods, chemicals

    measured_samples = set(measurements["sample_id"])
    keep_foods = foods["ptfi_sample_ids"].map(
        lambda ids: bool(measured_samples.intersection(ids))
    )
    keep_chemicals = chemicals["cid"].isin(set(measurements["cid"]))

    dropped_foods = int((~keep_foods).sum())
    dropped_chemicals = int((~keep_chemicals).sum())
    if dropped_foods or dropped_chemicals:
        logger.info(
            "PTFI: skipped %d foods and %d chemicals with no measurement.",
            dropped_foods,
            dropped_chemicals,
        )
    return foods[keep_foods].copy(), chemicals[keep_chemicals].copy()


# ---------------------------------------------------------------------- building
def _build_nodes(foods: pd.DataFrame, chemicals: pd.DataFrame) -> pd.DataFrame:
    food_nodes = pd.DataFrame(
        {
            "source_id": SOURCE_ID,
            "native_id": foods["native_id"],
            "name": foods["common_name"],
            "synonyms": foods["synonyms"],
            "synonym_types": [[] for _ in range(len(foods))],
            "node_type": "food",
            "raw_attrs": [
                {
                    "sample_ids": row["ptfi_sample_ids"],
                    "scientific_name": row["scientific_name"],
                    "food_groups": row["food_groups"],
                    "foodon_iri": row["foodon_iri"],
                }
                for _, row in foods.iterrows()
            ],
        }
    )
    chemical_nodes = pd.DataFrame(
        {
            "source_id": SOURCE_ID,
            "native_id": chemicals["native_id"],
            "name": chemicals["common_name"],
            "synonyms": [[n] if n else [] for n in chemicals["common_name"]],
            "synonym_types": [[] for _ in range(len(chemicals))],
            "node_type": "chemical",
            "raw_attrs": [
                {
                    "cid": row["cid"],
                    "ptfi_id": row["ptfi_id"],
                    "inchikey": row["inchikey"],
                }
                for _, row in chemicals.iterrows()
            ],
        }
    )
    return pd.concat([food_nodes, chemical_nodes], ignore_index=True)[NODES_COLUMNS]


def _build_xrefs(foods: pd.DataFrame, chemicals: pd.DataFrame) -> pd.DataFrame:
    """The external keys Pass 2 links on: FoodOn IRIs and PubChem CIDs."""
    with_iri = foods[foods["foodon_iri"] != ""]
    food_xrefs = pd.DataFrame(
        {
            "source_id": SOURCE_ID,
            "native_id": with_iri["native_id"],
            "target_source": "foodon",
            "target_id": with_iri["foodon_iri"],
        }
    )
    chemical_xrefs = pd.DataFrame(
        {
            "source_id": SOURCE_ID,
            "native_id": chemicals["native_id"],
            "target_source": "pubchem",
            "target_id": chemicals["cid"],
        }
    )
    inchi = chemicals[chemicals["inchikey"] != ""]
    inchi_xrefs = pd.DataFrame(
        {
            "source_id": SOURCE_ID,
            "native_id": inchi["native_id"],
            "target_source": "inchikey",
            "target_id": inchi["inchikey"],
        }
    )
    return pd.concat([food_xrefs, chemical_xrefs, inchi_xrefs], ignore_index=True)[
        XREFS_COLUMNS
    ]


def _build_edges(
    foods: pd.DataFrame, measurements: pd.DataFrame, evidence: dict
) -> pd.DataFrame:
    """One ``contains`` edge per measurement, head resolved to its food group.

    ``sample_id`` rides along in ``raw_attrs`` so the triplets stage can keep one
    attestation per physical sample even though the head is the collapsed group.
    """
    sample_to_food = {
        sample_id: native_id
        for native_id, sample_ids in zip(
            foods["native_id"], foods["ptfi_sample_ids"], strict=False
        )
        for sample_id in sample_ids
    }
    edges = measurements.copy()
    edges["head_native_id"] = edges["sample_id"].map(sample_to_food)

    unmapped = int(edges["head_native_id"].isna().sum())
    if unmapped:
        logger.warning("PTFI: dropped %d measurements with no food group.", unmapped)
        edges = edges[edges["head_native_id"].notna()]

    return pd.DataFrame(
        {
            "source_id": SOURCE_ID,
            "head_native_id": edges["head_native_id"],
            "tail_native_id": _CHEM_PREFIX + ":" + edges["cid"],
            "edge_type": "contains",
            "raw_attrs": [
                {
                    "sample_id": row["sample_id"],
                    "chemical_name": row["chemical_name"],
                    "conc_value": row["conc_value"],
                    "conc_unit": row["conc_unit"],
                    "dataset": evidence.get("dataset", ""),
                    "platform": evidence.get("platform", ""),
                }
                for _, row in edges.iterrows()
            ],
        }
    )[EDGES_COLUMNS]


def _write_outputs(
    output_dir: Path, nodes: pd.DataFrame, edges: pd.DataFrame, xrefs: pd.DataFrame
) -> list[str]:
    files = []
    for name, df in (("nodes", nodes), ("edges", edges), ("xrefs", xrefs)):
        path = output_dir / f"{SOURCE_ID}_{name}.parquet"
        serialize_raw_attrs(df).to_parquet(path, index=False)
        files.append(str(path))
    return files


# ---------------------------------------------------------------------- helpers
def _parse_list(cell: object) -> list:
    if isinstance(cell, list):
        return cell
    if isinstance(cell, str) and cell.strip():
        try:
            value = json.loads(cell)
        except json.JSONDecodeError:
            return []
        return value if isinstance(value, list) else []
    return []

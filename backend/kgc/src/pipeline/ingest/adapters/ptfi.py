"""PTFI adapter — foods, chemicals, and per-sample concentration measurements.

Reads the external-id-keyed source under ``data/PTFI/`` (written once per
delivery by ``scripts/prepare_delivery.py``) and emits the three standard
ingest artifacts. Keys are PTFI's own identifiers plus public chemical ids,
never FoodAtlas entity ids, so entity resolution does the FoodOn-IRI dedup and
id minting natively on every run.

* ``nodes``  — one food per PTFI food group, one chemical per measured key
* ``edges``  — one ``contains`` edge per (sample, chemical) measurement, carrying
  the value, its unit and the evidence record behind it; the triplets stage
  turns each into an attestation
* ``xrefs``  — food → FoodOn IRI; chemical → every public id it has
  (:data:`~src.pipeline.ingest.ptfi_keys.CHEMICAL_KEYS` plus InChIKey), the
  keys Pass 2 links on

PTFI reports two kinds of value — LC-MS relative abundance from untargeted
metabolomics and absolute ``mg/100g`` from its reference panels — each under
its own evidence record. Both pass through unconverted.

Only entities a measurement touches are emitted. PTFI identifies far more
chemicals than it detects, and a chemical with no measurement can carry no
triplet, so creating it would add an entity with no evidence behind it. Set
:data:`INCLUDE_UNMEASURED` to keep them.
"""

from __future__ import annotations

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
from ..ptfi_keys import CHEMICAL_KEYS
from .ptfi_source import (
    CHEM_PREFIX,
    read_chemicals,
    read_evidence,
    read_foods,
    read_measurements,
)

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "ptfi"

# Emit entities with no measurement behind them (see module docstring).
INCLUDE_UNMEASURED = False

_CHEMICAL_COLUMNS = [key.column for key in CHEMICAL_KEYS]
_CHEMICAL_XREFS = [
    *((key.column, key.source) for key in CHEMICAL_KEYS),
    ("inchikey", "inchikey"),
]


class PTFIAdapter:
    """Parse the prepared PTFI source into standardized ingest parquet."""

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

        foods = read_foods(ptfi_dir / "ptfi_foods.csv")
        chemicals = read_chemicals(ptfi_dir / "ptfi_chemicals.csv")
        measurements = read_measurements(ptfi_dir / "ptfi_measurements.csv")
        evidence = read_evidence(ptfi_dir / "ptfi_evidence.csv")

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
    keep_chemicals = chemicals["chemical_key"].isin(set(measurements["chemical_key"]))

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
                    column: row[column]
                    for column in (*_CHEMICAL_COLUMNS, "ptfi_id", "inchikey")
                }
                for _, row in chemicals.iterrows()
            ],
        }
    )
    return pd.concat([food_nodes, chemical_nodes], ignore_index=True)[NODES_COLUMNS]


def _build_xrefs(foods: pd.DataFrame, chemicals: pd.DataFrame) -> pd.DataFrame:
    """The external keys Pass 2 links on: a food's FoodOn IRI, every public id
    a chemical has."""
    with_iri = foods[foods["foodon_iri"] != ""]
    frames = [
        pd.DataFrame(
            {
                "source_id": SOURCE_ID,
                "native_id": with_iri["native_id"],
                "target_source": "foodon",
                "target_id": with_iri["foodon_iri"],
            }
        )
    ]
    for column, target_source in _CHEMICAL_XREFS:
        present = chemicals[chemicals[column] != ""]
        frames.append(
            pd.DataFrame(
                {
                    "source_id": SOURCE_ID,
                    "native_id": present["native_id"],
                    "target_source": target_source,
                    "target_id": present[column],
                }
            )
        )
    return pd.concat(frames, ignore_index=True)[XREFS_COLUMNS]


def _build_edges(
    foods: pd.DataFrame, measurements: pd.DataFrame, evidence: dict[str, dict]
) -> pd.DataFrame:
    """One ``contains`` edge per measurement, head resolved to its food group.

    ``sample_id`` and ``chemical_key`` ride along in ``raw_attrs`` so the
    triplets stage can keep one attestation per physical sample even though the
    head is the collapsed group, and resolve the chemical by whichever public
    id keys it.
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
            "tail_native_id": CHEM_PREFIX + ":" + edges["chemical_key"],
            "edge_type": "contains",
            "raw_attrs": [
                {
                    "sample_id": row["sample_id"],
                    "chemical_key": row["chemical_key"],
                    "chemical_name": row["chemical_name"],
                    "conc_value": row["conc_value"],
                    "conc_unit": row["conc_unit"],
                    "evidence": evidence[row["evidence_id"]],
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

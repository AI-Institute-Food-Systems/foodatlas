"""Bioactivity adapter — faithful ingest of bioactivity concepts, edges, and
per-measurement evidence.

Emits the three standard ingest artifacts (nodes / edges / xrefs) plus three
domain-specific passthrough files that downstream stages read by path:

* ``bioactivity_measurements.parquet`` — one row per ``bm…`` assay measurement
  (potency / efficacy / outcome). Consumed by the triplets stage as the
  evidence layer for ``exhibits`` / ``measured`` edges.
* ``bioactivity_disease.parquet`` / ``bioactivity_disease_targets.parquet`` —
  disease↔assay bridge and target metadata. Inert until the Phase-2 enrichment
  stage exists; staged here so this adapter is the only reader of raw CSVs.

No entity ids are minted and no native ids are resolved here — that is the
job of the entities and triplets stages.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.ingest import SourceManifest
from ..protocol import (
    EDGES_COLUMNS,
    XREFS_COLUMNS,
    ProgressCallback,
    _noop_progress,
    serialize_raw_attrs,
    write_manifest,
)

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "bioactivity"

# Verbose source columns → canonical measurement schema (see KG plan).
_MEASUREMENT_RENAME = {
    "evidence_value_potency_value": "potency_value",
    "evidence_value_potency_unit": "potency_unit",
    "evidence_value_efficacy_zeroactivity": "efficacy_zeroactivity",
    "evidence_value_efficacy_infiniteactivity": "efficacy_infiniteactivity",
    "evidence_value_efficacy_logac50_value": "efficacy_logac50_value",
    "evidence_value_efficacy_hillslope": "efficacy_hillslope",
}


class BioactivityAdapter:
    """Parse the Bioactivity CSVs into standardized ingest parquet."""

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
        bio_dir = raw_dir / "Bioactivity"

        concepts = pd.read_csv(bio_dir / "bioactivity_entities.csv")
        food_raw = pd.read_csv(bio_dir / "food_bioactivity_triplets.csv")
        chem_raw = pd.read_csv(bio_dir / "chemical_bioactivity_triplets.csv")

        total = len(concepts) + len(food_raw) + len(chem_raw)
        progress(0, total)

        nodes = _build_nodes(concepts)
        xrefs = _build_xrefs(concepts)
        progress(len(concepts), total)

        edges = _build_edges(concepts, food_raw, chem_raw)
        progress(total, total)

        measurements = _build_measurements(bio_dir / "bioactivity_metadata.csv")
        disease = _build_disease(bio_dir / "disease_bioactivity_triplets.csv")
        targets = _build_disease_targets(bio_dir / "bioactivity_disease_metadata.csv")

        files = _write_outputs(
            output_dir, nodes, edges, xrefs, measurements, disease, targets
        )

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
            "Bioactivity ingest: %d nodes, %d edges, %d xrefs, %d measurements.",
            len(nodes),
            len(edges),
            len(xrefs),
            len(measurements),
        )
        return manifest


def _build_nodes(concepts: pd.DataFrame) -> pd.DataFrame:
    """Build bioactivity-concept nodes (21 rows, row iteration is fine)."""
    rows: list[dict] = []
    for _, row in concepts.iterrows():
        name = str(row["common_name"]).lower().strip()
        synonyms = [name] if name else []
        synonym_types = ["name"] if name else []
        for syn in _parse_json_list(row["Synonyms"]):
            syn = syn.lower().strip()
            if syn and syn not in synonyms:
                synonyms.append(syn)
                synonym_types.append("synonym")

        rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": str(row["bioactivity_id"]),
                "name": name,
                "synonyms": synonyms,
                "synonym_types": synonym_types,
                "node_type": "bioactivity",
                "raw_attrs": {
                    "description": _clean_str(row.get("Description")),
                    "last_modified": _clean_str(row.get("last_modified")),
                    "external_database_ids": _parse_json_list(
                        row["External_database_IDs"]
                    ),
                    "parent_label_ids": _parse_comma_list(row["parent_label_ids"]),
                },
            }
        )
    return pd.DataFrame(rows)


def _build_xrefs(concepts: pd.DataFrame) -> pd.DataFrame:
    """Map each concept to its MeSH / ChEBI external ids."""
    rows: list[dict] = []
    for _, row in concepts.iterrows():
        native_id = str(row["bioactivity_id"])
        for ext in _parse_json_list(row["External_database_IDs"]):
            if ":" in ext:
                src, target_id = ext.split(":", 1)
                rows.append(
                    {
                        "source_id": SOURCE_ID,
                        "native_id": native_id,
                        "target_source": src.lower().strip(),
                        "target_id": target_id.strip(),
                    }
                )
    return pd.DataFrame(rows, columns=XREFS_COLUMNS)


def _build_edges(
    concepts: pd.DataFrame,
    food_raw: pd.DataFrame,
    chem_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Concatenate exhibits (food), measured (chemical), and is_a (hierarchy)."""
    food = _build_assoc_edges(food_raw, "foodatlas_id", "exhibits")
    chemical = _build_assoc_edges(chem_raw, "CID", "measured")
    hierarchy = _build_hierarchy_edges(concepts)
    return pd.concat([food, chemical, hierarchy], ignore_index=True)


def _build_assoc_edges(
    raw: pd.DataFrame, head_col: str, edge_type: str
) -> pd.DataFrame:
    """Build association edges, keeping each edge's ``bm…`` id list intact."""
    meta_ids = raw["bioactivity_metadata_ids"].apply(_parse_json_list)
    return pd.DataFrame(
        {
            "source_id": SOURCE_ID,
            "head_native_id": raw[head_col].astype(str),
            "tail_native_id": raw["bioactivity_id"].astype(str),
            "edge_type": edge_type,
            "raw_attrs": [{"bioactivity_metadata_ids": m} for m in meta_ids],
        }
    )


def _build_hierarchy_edges(concepts: pd.DataFrame) -> pd.DataFrame:
    """Build is_a edges from each concept's parent_label_ids."""
    rows: list[dict] = []
    for _, row in concepts.iterrows():
        child = str(row["bioactivity_id"])
        for parent in _parse_comma_list(row["parent_label_ids"]):
            rows.append(
                {
                    "source_id": SOURCE_ID,
                    "head_native_id": child,
                    "tail_native_id": parent,
                    "edge_type": "is_a",
                    "raw_attrs": {},
                }
            )
    return pd.DataFrame(rows, columns=EDGES_COLUMNS)


def _build_measurements(path: Path) -> pd.DataFrame:
    """Typed passthrough of the per-measurement assay table (~3.7M rows)."""
    df = pd.read_csv(path, low_memory=False)
    return df.rename(columns=_MEASUREMENT_RENAME)


def _build_disease(path: Path) -> pd.DataFrame:
    """Disease↔assay bridge passthrough (Phase-2 input)."""
    df = pd.read_csv(path)
    df["relationship"] = df["relationship"].apply(_parse_json_list)
    df["bioactivity_disease_metadata_id"] = df[
        "bioactivity_disease_metadata_id"
    ].apply(_parse_json_list)
    return df


def _build_disease_targets(path: Path) -> pd.DataFrame:
    """Disease-metadata → bridging target ids passthrough (Phase-2 input)."""
    df = pd.read_csv(path)
    df["target_ids"] = df["target_ids"].apply(_parse_json_list)
    return df


def _write_outputs(
    output_dir: Path,
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
    xrefs: pd.DataFrame,
    measurements: pd.DataFrame,
    disease: pd.DataFrame,
    targets: pd.DataFrame,
) -> list[str]:
    """Write all artifacts and return their paths for the manifest."""
    artifacts = {
        "nodes": serialize_raw_attrs(nodes),
        "edges": serialize_raw_attrs(edges),
        "xrefs": xrefs,
        "measurements": measurements,
        "disease": disease,
        "disease_targets": targets,
    }
    paths: list[str] = []
    for suffix, df in artifacts.items():
        path = output_dir / f"{SOURCE_ID}_{suffix}.parquet"
        df.to_parquet(path)
        paths.append(str(path))
    return paths


def _parse_json_list(cell: object) -> list[str]:
    """Parse a JSON-array CSV cell (e.g. Synonyms, bioactivity_metadata_ids)."""
    if not isinstance(cell, str) or not cell.strip():
        return []
    return json.loads(cell)


def _parse_comma_list(cell: object) -> list[str]:
    """Parse a plain comma-separated CSV cell (e.g. parent_label_ids)."""
    if not isinstance(cell, str) or not cell.strip():
        return []
    return [part.strip() for part in cell.split(",") if part.strip()]


def _clean_str(val: object) -> str:
    if not pd.notna(val):
        return ""
    if isinstance(val, float) and val.is_integer():
        return str(int(val))
    return str(val).strip()

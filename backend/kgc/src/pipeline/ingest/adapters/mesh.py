"""MeSH adapter — faithful ingest of MeSH descriptor and supplementary data."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
import xmltodict

from ....models.ingest import SourceManifest
from ..protocol import (
    ProgressCallback,
    _noop_progress,
    serialize_raw_attrs,
    write_manifest,
)

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "mesh"


class MeSHAdapter:
    """Parse MeSH XML files into standardized ingest parquet."""

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
        mesh_dir = raw_dir / "MeSH"
        desc_path = _find_file(mesh_dir, "desc*.xml")
        supp_path = _find_file(mesh_dir, "supp*.xml")

        nodes, edges = _build_outputs(desc_path, supp_path, progress)

        nodes = serialize_raw_attrs(nodes)
        edges = serialize_raw_attrs(edges)

        nodes_path = output_dir / f"{SOURCE_ID}_nodes.parquet"
        edges_path = output_dir / f"{SOURCE_ID}_edges.parquet"
        nodes.to_parquet(nodes_path)
        edges.to_parquet(edges_path)

        manifest = SourceManifest(
            source_id=SOURCE_ID,
            node_count=len(nodes),
            edge_count=len(edges),
            raw_dir=str(raw_dir),
            output_files=[str(nodes_path), str(edges_path)],
        )
        write_manifest(manifest, output_dir)
        logger.info("MeSH ingest: %d nodes, %d edges.", len(nodes), len(edges))
        return manifest


def _find_file(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    if not matches:
        msg = f"No file matching '{pattern}' in {directory}"
        raise FileNotFoundError(msg)
    return matches[-1]


def _build_outputs(
    desc_path: Path,
    supp_path: Path,
    progress: ProgressCallback = _noop_progress,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    node_rows: list[dict] = []
    edge_rows: list[dict] = []

    with desc_path.open() as f:
        desc_records = xmltodict.parse(f.read())["DescriptorRecordSet"][
            "DescriptorRecord"
        ]
    with supp_path.open() as f:
        supp_records = xmltodict.parse(f.read())["SupplementalRecordSet"][
            "SupplementalRecord"
        ]

    combined_total = len(desc_records) + len(supp_records)

    _parse_descriptors(desc_records, node_rows, edge_rows, progress, 0, combined_total)
    _parse_supplementals(
        supp_records, node_rows, edge_rows, progress, len(desc_records), combined_total
    )

    return pd.DataFrame(node_rows), pd.DataFrame(edge_rows)


def _parse_descriptors(
    records: list[dict],
    node_rows: list[dict],
    edge_rows: list[dict],
    progress: ProgressCallback = _noop_progress,
    offset: int = 0,
    combined_total: int = 0,
) -> None:
    for i, record in enumerate(records):
        mesh_id = record["DescriptorUI"]
        name = record["DescriptorName"]["String"]
        tree_numbers = (
            _ensure_list(record["TreeNumberList"]["TreeNumber"])
            if "TreeNumberList" in record
            else []
        )
        synonyms = _extract_synonyms(record)
        syn_types = ["label"] + ["synonym"] * (len(synonyms) - 1) if synonyms else []

        node_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": mesh_id,
                "name": name.lower().strip(),
                "synonyms": [s.lower() for s in synonyms],
                "synonym_types": syn_types,
                "node_type": "descriptor",
                "raw_attrs": {},
            }
        )

        for tn in tree_numbers:
            parts = tn.rsplit(".", 1)
            if len(parts) == 2:
                edge_rows.append(
                    {
                        "source_id": SOURCE_ID,
                        "head_native_id": mesh_id,
                        "tail_native_id": f"tree:{parts[0]}",
                        "edge_type": "tree_parent",
                        "raw_attrs": {},
                    }
                )

        progress(offset + i, combined_total)

    progress(offset + len(records), combined_total)


def _parse_supplementals(
    records: list[dict],
    node_rows: list[dict],
    edge_rows: list[dict],
    progress: ProgressCallback = _noop_progress,
    offset: int = 0,
    combined_total: int = 0,
) -> None:
    for i, record in enumerate(records):
        mesh_id = record["SupplementalRecordUI"]
        name = record["SupplementalRecordName"]["String"]
        mapped_to = _ensure_list(record["HeadingMappedToList"]["HeadingMappedTo"])
        mapped_ids = [x["DescriptorReferredTo"]["DescriptorUI"] for x in mapped_to]
        synonyms = _extract_synonyms(record)
        syn_types = ["label"] + ["synonym"] * (len(synonyms) - 1) if synonyms else []

        node_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": mesh_id,
                "name": name.lower().strip(),
                "synonyms": [s.lower() for s in synonyms],
                "synonym_types": syn_types,
                "node_type": "supplemental",
                "raw_attrs": {},
            }
        )

        for desc_id in mapped_ids:
            edge_rows.append(
                {
                    "source_id": SOURCE_ID,
                    "head_native_id": mesh_id,
                    "tail_native_id": desc_id,
                    "edge_type": "mapped_to",
                    "raw_attrs": {},
                }
            )

        progress(offset + i, combined_total)

    progress(offset + len(records), combined_total)


def _extract_synonyms(record: dict) -> list[str]:
    synonyms: list[str] = []
    concepts = _ensure_list(record["ConceptList"]["Concept"])
    for concept in concepts:
        terms = _ensure_list(concept["TermList"]["Term"])
        for term in terms:
            synonyms.append(term["String"])
    return synonyms


def _ensure_list(val: object) -> list:
    if isinstance(val, list):
        return val
    if isinstance(val, dict | str):
        return [val]
    msg = f"Unknown type: {type(val)}"
    raise ValueError(msg)

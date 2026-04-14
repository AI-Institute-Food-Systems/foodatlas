"""MeSH adapter — faithful ingest of MeSH descriptor and supplementary data."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from lxml import etree

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

        desc_count = _count_elements(desc_path, "DescriptorRecord")
        supp_count = _count_elements(supp_path, "SupplementalRecord")
        combined_total = desc_count + supp_count

        node_rows: list[dict] = []
        edge_rows: list[dict] = []

        _parse_descriptors(desc_path, node_rows, edge_rows, progress, 0, combined_total)
        _parse_supplementals(
            supp_path, node_rows, edge_rows, progress, desc_count, combined_total
        )

        nodes = serialize_raw_attrs(pd.DataFrame(node_rows))
        edges = serialize_raw_attrs(pd.DataFrame(edge_rows))

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


def _count_elements(path: Path, tag: str) -> int:
    """Fast count of top-level elements using iterparse (end events only)."""
    count = 0
    for _event, elem in etree.iterparse(path, events=("end",), tag=tag):
        count += 1
        elem.clear()
    return count


def _text(elem: etree._Element, path: str) -> str:
    child = elem.find(path)
    return child.text.strip() if child is not None and child.text else ""


def _parse_descriptors(
    path: Path,
    node_rows: list[dict],
    edge_rows: list[dict],
    progress: ProgressCallback,
    offset: int,
    combined_total: int,
) -> None:
    progress(offset, combined_total)
    for i, (_, elem) in enumerate(
        etree.iterparse(path, events=("end",), tag="DescriptorRecord")
    ):
        mesh_id = _text(elem, "DescriptorUI")
        name = _text(elem, "DescriptorName/String")
        tree_numbers = [tn.text for tn in elem.findall(".//TreeNumber") if tn.text]
        synonyms = _extract_synonyms(elem)
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

        elem.clear()
        progress(offset + i + 1, combined_total)


def _parse_supplementals(
    path: Path,
    node_rows: list[dict],
    edge_rows: list[dict],
    progress: ProgressCallback,
    offset: int,
    combined_total: int,
) -> None:
    for i, (_, elem) in enumerate(
        etree.iterparse(path, events=("end",), tag="SupplementalRecord")
    ):
        mesh_id = _text(elem, "SupplementalRecordUI")
        name = _text(elem, "SupplementalRecordName/String")
        mapped_ids = [
            _text(hm, "DescriptorReferredTo/DescriptorUI")
            for hm in elem.findall(".//HeadingMappedTo")
        ]
        synonyms = _extract_synonyms(elem)
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
            if desc_id:
                edge_rows.append(
                    {
                        "source_id": SOURCE_ID,
                        "head_native_id": mesh_id,
                        "tail_native_id": desc_id,
                        "edge_type": "mapped_to",
                        "raw_attrs": {},
                    }
                )

        elem.clear()
        progress(offset + i + 1, combined_total)

    progress(combined_total, combined_total)


def _extract_synonyms(record: etree._Element) -> list[str]:
    return [
        term.text
        for term in record.findall(".//Concept/TermList/Term/String")
        if term.text
    ]


def _ensure_list(val: object) -> list:
    if isinstance(val, list):
        return val
    if isinstance(val, dict | str):
        return [val]
    msg = f"Unknown type: {type(val)}"
    raise ValueError(msg)

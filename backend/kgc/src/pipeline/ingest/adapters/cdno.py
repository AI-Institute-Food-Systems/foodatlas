"""CDNO adapter — faithful ingest of the CDNO ontology."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd
from bs4 import BeautifulSoup

from ....models.ingest import SourceManifest
from ..protocol import serialize_raw_attrs, write_manifest

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "cdno"


class CDNOAdapter:
    """Parse CDNO OWL file into standardized ingest parquet."""

    @property
    def source_id(self) -> str:
        return SOURCE_ID

    def ingest(self, raw_dir: Path, output_dir: Path) -> SourceManifest:
        output_dir.mkdir(parents=True, exist_ok=True)
        owl_path = raw_dir / "CDNO" / "cdno.owl"

        nodes, edges, xrefs = _parse_cdno(owl_path)

        nodes = serialize_raw_attrs(nodes)
        edges = serialize_raw_attrs(edges)

        files: list[str] = []
        nodes_path = output_dir / f"{SOURCE_ID}_nodes.parquet"
        edges_path = output_dir / f"{SOURCE_ID}_edges.parquet"
        xrefs_path = output_dir / f"{SOURCE_ID}_xrefs.parquet"
        nodes.to_parquet(nodes_path)
        edges.to_parquet(edges_path)
        xrefs.to_parquet(xrefs_path)
        files = [str(nodes_path), str(edges_path), str(xrefs_path)]

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
            "CDNO ingest: %d nodes, %d edges, %d xrefs.",
            len(nodes),
            len(edges),
            len(xrefs),
        )
        return manifest


def _parse_cdno(
    owl_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with owl_path.open() as f:
        soup = BeautifulSoup(f, "xml")

    node_rows: list[dict] = []
    edge_rows: list[dict] = []
    xref_rows: list[dict] = []

    for class_ in soup.find_all("owl:Class"):
        cdno_id = class_.attrs.get("rdf:about")
        if cdno_id is None or class_.find("owl:deprecated"):
            continue

        label_el = class_.find("rdfs:label")
        label = label_el.text if label_el else ""

        node_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": cdno_id,
                "name": label.lower().strip(),
                "synonyms": [label.lower().strip()] if label else [],
                "synonym_types": ["label"] if label else [],
                "node_type": "class",
                "raw_attrs": {},
            }
        )

        for parent in class_.find_all("rdfs:subClassOf"):
            parent_id = parent.attrs.get("rdf:resource")
            if parent_id is not None:
                edge_rows.append(
                    {
                        "source_id": SOURCE_ID,
                        "head_native_id": cdno_id,
                        "tail_native_id": parent_id,
                        "edge_type": "is_a",
                        "raw_attrs": {},
                    }
                )

        for chebi_id in _extract_chebi_ids(class_):
            xref_rows.append(
                {
                    "source_id": SOURCE_ID,
                    "native_id": cdno_id,
                    "target_source": "chebi",
                    "target_id": chebi_id,
                }
            )

        for fdc_id in _extract_fdc_ids(class_):
            xref_rows.append(
                {
                    "source_id": SOURCE_ID,
                    "native_id": cdno_id,
                    "target_source": "fdc_nutrient",
                    "target_id": str(fdc_id),
                }
            )

    return pd.DataFrame(node_rows), pd.DataFrame(edge_rows), pd.DataFrame(xref_rows)


def _extract_chebi_ids(class_element: Any) -> list[str]:
    chebi_ids: list[str] = []
    for eq in class_element.find_all("owl:equivalentClass"):
        for desc in eq.find_all("rdf:Description"):
            about = desc.attrs.get("rdf:about", "")
            if "CHEBI_" in about:
                chebi_ids.append(about)
    return chebi_ids


def _extract_fdc_ids(class_element: Any) -> list[str]:
    return [
        ref.text.split("USDA_fdc_id:")[-1]
        for ref in class_element.find_all("oboInOwl:hasDbXref")
        if ref.text.startswith("USDA_fdc_id")
    ]

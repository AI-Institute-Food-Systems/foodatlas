"""CTD adapter — faithful ingest of CTD chemical-disease and disease data."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.ingest import SourceManifest
from ..protocol import serialize_raw_attrs, write_manifest

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "ctd"

_COLUMNS_WITH_LISTS = [
    "OmimIDs",
    "PubMedIDs",
    "ParentIDs",
    "TreeNumbers",
    "ParentTreeNumbers",
    "Synonyms",
    "AltDiseaseIDs",
    "SlimMappings",
]


class CTDAdapter:
    """Parse CTD CSV files into standardized ingest parquet."""

    @property
    def source_id(self) -> str:
        return SOURCE_ID

    def ingest(self, raw_dir: Path, output_dir: Path) -> SourceManifest:
        output_dir.mkdir(parents=True, exist_ok=True)
        ctd_dir = raw_dir / "CTD"

        chemdis = _load_ctd_csv(ctd_dir / "CTD_chemicals_diseases.csv")
        diseases = _load_ctd_csv(ctd_dir / "CTD_diseases.csv")

        nodes, xrefs = _build_disease_nodes(diseases)
        disease_edges = _build_disease_edges(diseases)
        chemdis_edges = _build_chemdis_edges(chemdis)
        edges = pd.concat([disease_edges, chemdis_edges], ignore_index=True)

        nodes = serialize_raw_attrs(nodes)
        edges = serialize_raw_attrs(edges)

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
            "CTD ingest: %d nodes, %d edges, %d xrefs.",
            len(nodes),
            len(edges),
            len(xrefs),
        )
        return manifest


def _build_disease_nodes(
    diseases: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build nodes and xrefs from disease table (small, row iteration OK)."""
    node_rows: list[dict] = []
    xref_rows: list[dict] = []

    for _, row in diseases.iterrows():
        syns = row.get("Synonyms", [])
        if not isinstance(syns, list):
            syns = []
        raw_name = row.get("DiseaseName")
        name = str(raw_name).lower().strip() if pd.notna(raw_name) else ""
        node_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": str(row["DiseaseID"]),
                "name": name,
                "synonyms": [s.lower() for s in syns] if syns else [],
                "synonym_types": ["synonym"] * len(syns) if syns else [],
                "node_type": "disease",
                "raw_attrs": {},
            }
        )
        for alt_id in row.get("AltDiseaseIDs", []):
            alt_str = str(alt_id)
            if ":" in alt_str:
                src, tid = alt_str.split(":", 1)
                xref_rows.append(
                    {
                        "source_id": SOURCE_ID,
                        "native_id": str(row["DiseaseID"]),
                        "target_source": src.lower(),
                        "target_id": tid,
                    }
                )

    return pd.DataFrame(node_rows), pd.DataFrame(xref_rows)


def _build_disease_edges(diseases: pd.DataFrame) -> pd.DataFrame:
    """Build is_a edges from disease parent IDs (small, row iteration OK)."""
    rows: list[dict] = []
    for _, row in diseases.iterrows():
        for parent in row.get("ParentIDs", []):
            if parent:
                rows.append(
                    {
                        "source_id": SOURCE_ID,
                        "head_native_id": str(row["DiseaseID"]),
                        "tail_native_id": str(parent),
                        "edge_type": "is_a",
                        "raw_attrs": {},
                    }
                )
    return pd.DataFrame(rows)


def _build_chemdis_edges(chemdis: pd.DataFrame) -> pd.DataFrame:
    """Build chemical-disease edges vectorized (9M rows)."""
    result = pd.DataFrame(
        {
            "source_id": SOURCE_ID,
            "head_native_id": chemdis["ChemicalID"].astype(str),
            "tail_native_id": chemdis["DiseaseID"].astype(str),
            "edge_type": "chemical_disease_association",
        }
    )
    de = chemdis["DirectEvidence"].fillna("")
    result["raw_attrs"] = de.apply(lambda x: {"direct_evidence": x})
    return result


def _load_ctd_csv(file_path: Path) -> pd.DataFrame:
    with file_path.open() as f:
        lines = f.readlines()
        fields_idx = next(
            i for i, line in enumerate(lines) if line.strip() == "# Fields:"
        )
        header_idx = fields_idx + 1
        header = lines[header_idx].strip().replace("# ", "").split(",")

    df = pd.read_csv(
        file_path, comment="#", skiprows=range(1, header_idx), names=header
    )
    df = df.dropna(how="all").reset_index(drop=True)
    return _split_pipe_columns(df)


def _split_pipe_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in _COLUMNS_WITH_LISTS:
        if column not in df.columns:
            continue
        df[column] = df[column].apply(
            lambda x: str(x).split("|") if pd.notnull(x) else []
        )
        df[column] = df[column].apply(
            lambda x: [int(i) if isinstance(i, str) and i.isdigit() else i for i in x]
        )
    return df

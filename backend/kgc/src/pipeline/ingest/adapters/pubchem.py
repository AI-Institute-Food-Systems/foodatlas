"""PubChem adapter — ingest xref mappings (ChEBI-PubChem, CID-MeSH)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

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

SOURCE_ID = "pubchem"


class PubChemAdapter:
    """Parse PubChem SID-Map and CID-MeSH into cross-reference parquet."""

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
        pubchem_dir = raw_dir / "PubChem"

        xrefs = _build_xrefs(pubchem_dir, progress)
        xrefs = serialize_raw_attrs(xrefs)

        xrefs_path = output_dir / f"{SOURCE_ID}_xrefs.parquet"
        xrefs.to_parquet(xrefs_path)

        manifest = SourceManifest(
            source_id=SOURCE_ID,
            xref_count=len(xrefs),
            raw_dir=str(raw_dir),
            output_files=[str(xrefs_path)],
        )
        write_manifest(manifest, output_dir)
        logger.info("PubChem ingest: %d xrefs.", len(xrefs))
        return manifest


def _build_xrefs(
    pubchem_dir: Path,
    progress: ProgressCallback = _noop_progress,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []

    sid_map_path = pubchem_dir / "SID-Map"
    if sid_map_path.exists():
        chebi_sids = _read_chebi_sids(sid_map_path, progress)
        if not chebi_sids.empty:
            chebi_xrefs = pd.DataFrame(
                {
                    "source_id": SOURCE_ID,
                    "native_id": chebi_sids["cid"].astype(int).astype(str),
                    "target_source": "chebi",
                    "target_id": chebi_sids["registry_id"].astype(str),
                }
            )
            parts.append(chebi_xrefs)
        progress(len(chebi_sids), len(chebi_sids))

    cid_mesh_path = pubchem_dir / "CID-MeSH.txt"
    if cid_mesh_path.exists():
        cid_mesh = pd.read_csv(
            cid_mesh_path,
            sep="\t",
            names=["cid", "mesh_term", "mesh_term_alt"],
        )
        mesh_xrefs = pd.DataFrame(
            {
                "source_id": SOURCE_ID,
                "native_id": cid_mesh["cid"].astype(str),
                "target_source": "mesh_term",
                "target_id": cid_mesh["mesh_term"].astype(str),
            }
        )
        parts.append(mesh_xrefs)

    if parts:
        return pd.concat(parts, ignore_index=True)
    return pd.DataFrame()


def _read_chebi_sids(
    sid_map_path: Path,
    progress: ProgressCallback = _noop_progress,
) -> pd.DataFrame:
    """Read only ChEBI rows from the SID-Map using chunked filtering.

    The SID-Map is 15GB / 318M rows but only ~188K are ChEBI.
    Chunked reading avoids loading the full file into memory.
    """
    chunks: list[pd.DataFrame] = []
    chunk_size = 5_000_000
    reader = pd.read_csv(
        sid_map_path,
        sep="\t",
        header=None,
        names=["SID", "source", "registry_id", "cid"],
        dtype={"registry_id": str, "SID": str},
        chunksize=chunk_size,
    )
    rows_read = 0
    total_estimate = 318_000_000
    for chunk in reader:
        chebi = chunk[chunk["source"] == "ChEBI"].dropna(subset=["cid"])
        if not chebi.empty:
            chunks.append(chebi)
        rows_read += len(chunk)
        progress(rows_read, total_estimate)

    progress(total_estimate, total_estimate)
    if chunks:
        return pd.concat(chunks, ignore_index=True)
    return pd.DataFrame(columns=["SID", "source", "registry_id", "cid"])

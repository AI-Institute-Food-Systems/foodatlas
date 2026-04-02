"""PubChem adapter — ingest cross-reference mappings (ChEBI ↔ PubChem, CID ↔ MeSH)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.ingest import SourceManifest
from ..protocol import serialize_raw_attrs, write_manifest

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "pubchem"


class PubChemAdapter:
    """Parse PubChem SID-Map and CID-MeSH into cross-reference parquet."""

    @property
    def source_id(self) -> str:
        return SOURCE_ID

    def ingest(self, raw_dir: Path, output_dir: Path) -> SourceManifest:
        output_dir.mkdir(parents=True, exist_ok=True)
        pubchem_dir = raw_dir / "PubChem"

        xrefs = _build_xrefs(pubchem_dir)
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


def _build_xrefs(pubchem_dir: Path) -> pd.DataFrame:
    rows: list[dict] = []

    sid_map_path = pubchem_dir / "SID-Map"
    if sid_map_path.exists():
        sids = pd.read_csv(
            sid_map_path,
            sep="\t",
            header=None,
            names=["SID", "source", "registry_id", "cid"],
        )
        chebi_sids = sids[sids["source"] == "ChEBI"].copy()
        for _, row in chebi_sids.iterrows():
            if pd.notna(row["cid"]):
                rows.append(
                    {
                        "source_id": SOURCE_ID,
                        "native_id": str(int(row["cid"])),
                        "target_source": "chebi",
                        "target_id": str(row["registry_id"]),
                    }
                )

    cid_mesh_path = pubchem_dir / "CID-MeSH.txt"
    if cid_mesh_path.exists():
        cid_mesh = pd.read_csv(
            cid_mesh_path,
            sep="\t",
            names=["cid", "mesh_term", "mesh_term_alt"],
        )
        for _, row in cid_mesh.iterrows():
            rows.append(
                {
                    "source_id": SOURCE_ID,
                    "native_id": str(row["cid"]),
                    "target_source": "mesh_term",
                    "target_id": str(row["mesh_term"]),
                }
            )

    return pd.DataFrame(rows)

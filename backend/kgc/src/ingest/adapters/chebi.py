"""ChEBI adapter — faithful ingest of ChEBI compounds and relations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.ingest import SourceManifest
from ..protocol import serialize_raw_attrs, write_manifest

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "chebi"


class ChEBIAdapter:
    """Parse ChEBI TSV files into standardized ingest parquet."""

    @property
    def source_id(self) -> str:
        return SOURCE_ID

    def ingest(self, raw_dir: Path, output_dir: Path) -> SourceManifest:
        output_dir.mkdir(parents=True, exist_ok=True)
        chebi_dir = raw_dir / "ChEBI"

        nodes = _build_nodes(chebi_dir)
        edges = _build_edges(chebi_dir)

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
        logger.info("ChEBI ingest: %d nodes, %d edges.", len(nodes), len(edges))
        return manifest


def _build_nodes(chebi_dir: Path) -> pd.DataFrame:
    compounds = pd.read_csv(
        chebi_dir / "compounds.tsv", sep="\t", encoding="latin1"
    ).set_index("ID")
    compounds = compounds[compounds["PARENT_ID"].isna()].copy()
    compounds["NAME"] = compounds["NAME"].str.lower().str.strip()

    synonyms_df = pd.read_csv(chebi_dir / "names.tsv", sep="\t")
    synonyms_df = synonyms_df.dropna(subset=["NAME"])
    synonyms_df = synonyms_df[synonyms_df["LANGUAGE"] == "en"].copy()
    synonyms_df["NAME"] = synonyms_df["NAME"].str.lower().str.strip()

    syn_by_id: dict[int, list[tuple[str, str]]] = {}
    for _, row in synonyms_df.iterrows():
        cid = row["COMPOUND_ID"]
        if cid not in syn_by_id:
            syn_by_id[cid] = []
        syn_by_id[cid].append((row["NAME"], row["TYPE"]))

    rows: list[dict] = []
    for chebi_id, row in compounds.iterrows():
        name = row["NAME"] if pd.notna(row["NAME"]) else ""
        star = int(row["STAR"]) if pd.notna(row["STAR"]) else 0
        syns = [name] if name else []
        syn_types = ["name"] if name else []
        for syn_name, syn_type in syn_by_id.get(int(chebi_id), []):
            if syn_name not in syns:
                syns.append(syn_name)
                syn_types.append(syn_type.lower() if pd.notna(syn_type) else "synonym")

        rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": str(chebi_id),
                "name": name,
                "synonyms": syns,
                "synonym_types": syn_types,
                "node_type": "compound",
                "raw_attrs": {"star": star},
            }
        )
    return pd.DataFrame(rows)


def _build_edges(chebi_dir: Path) -> pd.DataFrame:
    relations = pd.read_csv(chebi_dir / "relation.tsv", sep="\t")

    rows: list[dict] = []
    for _, row in relations.iterrows():
        edge_type = str(row["TYPE"]).lower().replace(" ", "_")
        rows.append(
            {
                "source_id": SOURCE_ID,
                "head_native_id": str(row["INIT_ID"]),
                "tail_native_id": str(row["FINAL_ID"]),
                "edge_type": edge_type,
                "raw_attrs": {},
            }
        )
    return pd.DataFrame(rows)

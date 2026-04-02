"""FoodOn adapter — faithful ingest of the FoodOn ontology."""

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

SOURCE_ID = "foodon"


class FoodOnAdapter:
    """Parse FoodOn synonym TSV into standardized ingest parquet.

    Only ``is_a`` edges are extracted (from the parent column in the TSV).
    """

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
        synonyms_path = raw_dir / "FoodOn" / "foodon-synonyms.tsv"

        nodes, edges = _build_nodes_and_edges(synonyms_path, progress)

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
        logger.info("FoodOn ingest: %d nodes, %d edges.", len(nodes), len(edges))
        return manifest


def _build_nodes_and_edges(
    synonyms_path: Path,
    progress: ProgressCallback = _noop_progress,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_csv(synonyms_path, sep="\t")
    raw["?class"] = raw["?class"].apply(_remove_brackets)
    raw["?parent"] = raw["?parent"].apply(_remove_brackets)

    groups = list(raw.groupby("?class"))
    total = len(groups)
    node_rows: list[dict] = []
    edge_rows: list[dict] = []

    for i, (foodon_id, group) in enumerate(groups):
        syns, syn_types = _parse_synonyms(group)
        parents = group["?parent"].dropna().unique().tolist()

        label_syns = [s for s, t in zip(syns, syn_types, strict=True) if t == "label"]
        first = label_syns[0] if label_syns else (syns[0] if syns else "")
        name = first.lower()

        node_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": str(foodon_id),
                "name": name,
                "synonyms": syns,
                "synonym_types": syn_types,
                "node_type": "class",
                "raw_attrs": {},
            }
        )
        for parent in parents:
            edge_rows.append(
                {
                    "source_id": SOURCE_ID,
                    "head_native_id": str(foodon_id),
                    "tail_native_id": str(parent),
                    "edge_type": "is_a",
                    "raw_attrs": {},
                }
            )

        progress(i, total)

    progress(total, total)
    return pd.DataFrame(node_rows), pd.DataFrame(edge_rows)


def _parse_synonyms(group: pd.DataFrame) -> tuple[list[str], list[str]]:
    syns: list[str] = []
    syn_types: list[str] = []
    type_map = {
        "label": "label",
        "label (alternative)": "label_alt",
        "synonym (exact)": "exact",
        "synonym": "synonym",
        "synonym (narrow)": "narrow",
        "synonym (broad)": "broad",
        "taxon": "taxon",
    }
    for _, row in group.dropna(subset=["?type"]).iterrows():
        raw_label = _remove_suffix(str(row["?label"]))
        mapped_type = type_map.get(row["?type"], row["?type"])
        syns.append(raw_label)
        syn_types.append(mapped_type)
    return syns, syn_types


def _remove_brackets(x: object) -> object:
    if pd.isna(x):
        return x
    s = str(x)
    return s[1:-1] if s.startswith("<") and s.endswith(">") else s


def _remove_suffix(x: str) -> str:
    for sep in ("@", "^^"):
        if sep in x:
            return x.split(sep, maxsplit=1)[0]
    return x

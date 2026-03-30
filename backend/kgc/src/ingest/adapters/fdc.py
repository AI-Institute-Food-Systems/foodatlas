"""FDC adapter — faithful ingest of USDA Food Data Central."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ...models.ingest import SourceManifest
from ..protocol import serialize_raw_attrs, write_manifest

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "fdc"


class FDCAdapter:
    """Parse FDC CSV files into standardized ingest parquet."""

    @property
    def source_id(self) -> str:
        return SOURCE_ID

    def ingest(self, raw_dir: Path, output_dir: Path) -> SourceManifest:
        output_dir.mkdir(parents=True, exist_ok=True)
        fdc_dir = _find_fdc_subdir(raw_dir / "FDC")

        nodes, edges, xrefs = _build_outputs(fdc_dir)

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
            "FDC ingest: %d nodes, %d edges, %d xrefs.",
            len(nodes),
            len(edges),
            len(xrefs),
        )
        return manifest


def _find_fdc_subdir(fdc_parent: Path) -> Path:
    matches = sorted(fdc_parent.glob("FoodData_Central_*"))
    if not matches:
        msg = f"No FDC data directory found in {fdc_parent}"
        raise FileNotFoundError(msg)
    return matches[-1]


def _build_outputs(
    fdc_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    node_rows: list[dict] = []
    edge_rows: list[dict] = []
    xref_rows: list[dict] = []

    ff_ids = set(pd.read_csv(fdc_dir / "foundation_food.csv")["fdc_id"])

    _add_food_nodes(fdc_dir, ff_ids, node_rows, xref_rows)
    _add_nutrient_nodes(fdc_dir, node_rows)
    _add_food_nutrient_edges(fdc_dir, ff_ids, edge_rows)

    return pd.DataFrame(node_rows), pd.DataFrame(edge_rows), pd.DataFrame(xref_rows)


def _add_food_nodes(
    fdc_dir: Path,
    ff_ids: set[int],
    node_rows: list[dict],
    xref_rows: list[dict],
) -> None:
    foods = pd.read_csv(
        fdc_dir / "food.csv", usecols=["fdc_id", "description"]
    ).set_index("fdc_id")
    foods = foods[foods.index.isin(ff_ids)]
    foods["description"] = foods["description"].str.strip().str.lower()

    food_attr = pd.read_csv(fdc_dir / "food_attribute.csv")
    food_attr = food_attr[food_attr["fdc_id"].isin(ff_ids)]

    foodon_attr = food_attr[
        food_attr["name"] == "FoodOn Ontology ID for FDC Item"
    ].copy()
    fdc_to_foodon: dict[int, list[str]] = {}
    for _, row in foodon_attr.iterrows():
        fdc_id = int(row["fdc_id"])
        if fdc_id not in fdc_to_foodon:
            fdc_to_foodon[fdc_id] = []
        fdc_to_foodon[fdc_id].append(str(row["value"]))

    for fdc_id, row in foods.iterrows():
        node_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": f"food:{fdc_id}",
                "name": row["description"],
                "synonyms": [row["description"]],
                "synonym_types": ["label"],
                "node_type": "food",
                "raw_attrs": {},
            }
        )
        for foodon_url in fdc_to_foodon.get(int(fdc_id), []):
            xref_rows.append(
                {
                    "source_id": SOURCE_ID,
                    "native_id": f"food:{fdc_id}",
                    "target_source": "foodon",
                    "target_id": foodon_url,
                }
            )


def _add_nutrient_nodes(fdc_dir: Path, node_rows: list[dict]) -> None:
    nutrients = pd.read_csv(
        fdc_dir / "nutrient.csv", usecols=["id", "name", "unit_name"]
    ).set_index("id")
    nutrients["name"] = nutrients["name"].str.lower().str.strip()

    for nutrient_id, row in nutrients.iterrows():
        node_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": f"nutrient:{nutrient_id}",
                "name": row["name"],
                "synonyms": [row["name"]],
                "synonym_types": ["label"],
                "node_type": "nutrient",
                "raw_attrs": {"unit_name": row["unit_name"]},
            }
        )


def _add_food_nutrient_edges(
    fdc_dir: Path, ff_ids: set[int], edge_rows: list[dict]
) -> None:
    food_nutrient = pd.read_csv(
        fdc_dir / "food_nutrient.csv",
        usecols=["id", "fdc_id", "nutrient_id", "amount"],
    )
    food_nutrient = food_nutrient[food_nutrient["fdc_id"].isin(ff_ids)]

    for _, row in food_nutrient.iterrows():
        edge_rows.append(
            {
                "source_id": SOURCE_ID,
                "head_native_id": f"food:{int(row['fdc_id'])}",
                "tail_native_id": f"nutrient:{int(row['nutrient_id'])}",
                "edge_type": "contains",
                "raw_attrs": {
                    "amount": float(row["amount"]) if pd.notna(row["amount"]) else 0.0,
                },
            }
        )

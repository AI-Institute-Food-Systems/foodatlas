"""Tests for FDC ingest adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.ingest.adapters.fdc import FDCAdapter

if TYPE_CHECKING:
    from pathlib import Path


def _setup_fdc_dir(tmp_path: Path) -> Path:
    fdc_parent = tmp_path / "FDC"
    fdc_parent.mkdir()
    fdc_dir = fdc_parent / "FoodData_Central_test_2024"
    fdc_dir.mkdir()

    pd.DataFrame({"fdc_id": [100, 200]}).to_csv(
        fdc_dir / "foundation_food.csv", index=False
    )
    pd.DataFrame(
        {
            "fdc_id": [100, 200, 300],
            "description": ["Apple", "Banana", "Not foundation"],
        }
    ).to_csv(fdc_dir / "food.csv", index=False)

    pd.DataFrame(
        {
            "fdc_id": [100, 100],
            "name": ["FoodOn Ontology ID for FDC Item", "Other Attr"],
            "value": ["http://foodon/F1", "something"],
        }
    ).to_csv(fdc_dir / "food_attribute.csv", index=False)

    pd.DataFrame(
        {
            "id": [1001, 1002],
            "name": ["Vitamin C", "Protein"],
            "unit_name": ["mg", "g"],
        }
    ).to_csv(fdc_dir / "nutrient.csv", index=False)

    pd.DataFrame(
        {
            "id": [1, 2, 3],
            "fdc_id": [100, 100, 200],
            "nutrient_id": [1001, 1002, 1001],
            "amount": [5.0, 10.0, 3.0],
        }
    ).to_csv(fdc_dir / "food_nutrient.csv", index=False)

    return fdc_parent


def test_fdc_adapter(tmp_path: Path) -> None:
    _setup_fdc_dir(tmp_path)

    adapter = FDCAdapter()
    out_dir = tmp_path / "output"
    manifest = adapter.ingest(tmp_path, out_dir)

    assert manifest.source_id == "fdc"
    assert manifest.node_count >= 4  # 2 foods + 2 nutrients
    assert manifest.edge_count >= 3  # 3 food_nutrient rows

    nodes = pd.read_parquet(out_dir / "fdc_nodes.parquet")
    foods = nodes[nodes["node_type"] == "food"]
    nutrients = nodes[nodes["node_type"] == "nutrient"]
    assert len(foods) == 2
    assert len(nutrients) == 2

    edges = pd.read_parquet(out_dir / "fdc_edges.parquet")
    assert all(edges["edge_type"] == "contains")
    assert len(edges) == 3

    xrefs = pd.read_parquet(out_dir / "fdc_xrefs.parquet")
    foodon_xrefs = xrefs[xrefs["target_source"] == "foodon"]
    assert len(foodon_xrefs) == 1
    assert foodon_xrefs.iloc[0]["target_id"] == "http://foodon/F1"


def test_fdc_adapter_missing_dir(tmp_path: Path) -> None:
    fdc_parent = tmp_path / "FDC"
    fdc_parent.mkdir()
    adapter = FDCAdapter()
    with pytest.raises(FileNotFoundError):
        adapter.ingest(tmp_path, tmp_path / "output")

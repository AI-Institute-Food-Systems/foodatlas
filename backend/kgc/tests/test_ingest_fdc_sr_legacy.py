"""Tests for the SR Legacy half of the FDC adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.models.settings import KGCSettings
from src.pipeline.ingest.adapters.fdc import FDCAdapter
from src.pipeline.ingest.adapters.fdc_sr_legacy import DATA_TYPE
from src.pipeline.ingest.protocol import deserialize_raw_attrs

from tests.test_ingest_fdc import _setup_fdc_dir

if TYPE_CHECKING:
    from pathlib import Path

FOODON = "http://purl.obolibrary.org/obo/FOODON_03302103"


def _setup_sr_dir(tmp_path: Path) -> Path:
    sr_dir = tmp_path / "FDC" / "sr_legacy_test"
    sr_dir.mkdir(parents=True)
    pd.DataFrame({"fdc_id": [500, 600], "NDB_number": ["01001", "13000"]}).to_csv(
        sr_dir / "sr_legacy_food.csv", index=False
    )
    pd.DataFrame(
        {
            "fdc_id": [500, 600, 700],
            "description": ["Butter, salted", "Beef, ground, 80% lean", "Branded"],
            "food_category_id": [1, 13, 25],
        }
    ).to_csv(sr_dir / "food.csv", index=False)
    pd.DataFrame(
        {"id": [1, 13], "code": ["0100", "1300"], "description": ["Dairy", "Beef"]}
    ).to_csv(sr_dir / "food_category.csv", index=False)
    pd.DataFrame(
        {
            "fdc_id": [500, 500, 500, 500, 600],
            "name": [
                "FoodOn Ontology ID #2 For FDC Item",
                "FoodOn Ontology ID #1 For FDC Item",
                "NCBI Taxon",
                "Scientific Name",
                "Note",
            ],
            "value": ["http://second", FOODON, "9913", "Bos taurus", "ignored"],
        }
    ).to_csv(sr_dir / "food_attribute.csv", index=False)
    pd.DataFrame(
        {
            "fdc_id": [500, 500, 600, 700],
            "nutrient_id": [1001, 1002, 1001, 1001],
            "amount": [0.5, None, 17.0, 1.0],
        }
    ).to_csv(sr_dir / "food_nutrient.csv", index=False)
    return sr_dir


def test_sr_legacy_foods_become_separate_nodes_without_xrefs(tmp_path: Path) -> None:
    _setup_fdc_dir(tmp_path)
    _setup_sr_dir(tmp_path)

    out = tmp_path / "out"
    FDCAdapter(sr_legacy_dir="FDC/sr_legacy_test").ingest(tmp_path, out)

    nodes = deserialize_raw_attrs(pd.read_parquet(out / "fdc_nodes.parquet"))
    foods = nodes[nodes["node_type"] == "food"].set_index("native_id")
    assert set(foods.index) == {"food:100", "food:200", "food:500", "food:600"}

    butter = foods.loc["food:500"]
    assert butter["name"] == "butter, salted"
    assert butter["raw_attrs"] == {
        "fdc_data_type": DATA_TYPE,
        "ndb_number": "01001",
        "fdc_category": "Dairy",
        "foodon_terms": [FOODON, "http://second"],
        "ncbi_taxon": "9913",
        "scientific_name": "Bos taurus",
    }
    assert foods.loc["food:600", "raw_attrs"]["fdc_category"] == "Beef"

    xrefs = pd.read_parquet(out / "fdc_xrefs.parquet")
    assert set(xrefs["native_id"]) == {"food:100"}, "SR foods never link to FoodOn"

    edges = deserialize_raw_attrs(pd.read_parquet(out / "fdc_edges.parquet"))
    sr_edges = edges[edges["head_native_id"].isin(["food:500", "food:600"])]
    assert len(sr_edges) == 3, "only SR Legacy rows of food_nutrient.csv are kept"
    by_key = {
        (r["head_native_id"], r["tail_native_id"]): r["raw_attrs"]["amount"]
        for _, r in sr_edges.iterrows()
    }
    assert by_key[("food:500", "nutrient:1001")] == 0.5
    assert by_key[("food:500", "nutrient:1002")] == 0.0, "missing amount reads as 0"
    assert by_key[("food:600", "nutrient:1001")] == 17.0


def test_sr_legacy_is_off_by_default(tmp_path: Path) -> None:
    _setup_fdc_dir(tmp_path)
    _setup_sr_dir(tmp_path)
    out = tmp_path / "out"
    FDCAdapter().ingest(tmp_path, out)
    nodes = pd.read_parquet(out / "fdc_nodes.parquet")
    assert "food:500" not in set(nodes["native_id"])


def test_sr_legacy_dir_without_crosswalk_fails_fast(tmp_path: Path) -> None:
    _setup_fdc_dir(tmp_path)
    (tmp_path / "FDC" / "empty").mkdir()
    with pytest.raises(FileNotFoundError, match=r"sr_legacy_food\.csv"):
        FDCAdapter(sr_legacy_dir="FDC/empty").ingest(tmp_path, tmp_path / "out")


def test_settings_expose_adapter_options() -> None:
    assert KGCSettings(fdc_sr_legacy_dir="").adapter_options == {}
    assert KGCSettings(fdc_sr_legacy_dir="FDC/sr").adapter_options == {
        "fdc": {"sr_legacy_dir": "FDC/sr"}
    }

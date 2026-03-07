"""Tests for FDC integration module."""

import pandas as pd
import pytest
from src.integration.triplets.fdc import (
    build_fdc_id_map,
    build_fdc_metadata,
    build_fdc_nutrient_id_map,
    load_fdc_nutrients,
)


class TestBuildFdcNutrientIdMap:
    def test_maps_fdc_nutrient_ids(self):
        entities = pd.DataFrame(
            [
                {"external_ids": {"fdc_nutrient": [1001, 1002]}},
                {"external_ids": {"fdc_nutrient": [1003]}},
            ],
            index=pd.Index(["e0", "e1"], name="foodatlas_id"),
        )
        result = build_fdc_nutrient_id_map(entities)
        assert result == {1001: "e0", 1002: "e0", 1003: "e1"}

    def test_skips_entities_without_fdc_nutrient(self):
        entities = pd.DataFrame(
            [{"external_ids": {"pubchem_cid": [123]}}],
            index=pd.Index(["e0"], name="foodatlas_id"),
        )
        assert build_fdc_nutrient_id_map(entities) == {}

    def test_duplicate_raises_value_error(self):
        entities = pd.DataFrame(
            [
                {"external_ids": {"fdc_nutrient": [1001]}},
                {"external_ids": {"fdc_nutrient": [1001]}},
            ],
            index=pd.Index(["e0", "e1"], name="foodatlas_id"),
        )
        with pytest.raises(ValueError, match="Duplicate FDC nutrient ID"):
            build_fdc_nutrient_id_map(entities)


class TestBuildFdcIdMap:
    def test_maps_fdc_ids(self):
        entities = pd.DataFrame(
            [{"external_ids": {"fdc": [100, 200]}}],
            index=pd.Index(["e0"], name="foodatlas_id"),
        )
        result = build_fdc_id_map(entities)
        assert result == {100: "e0", 200: "e0"}

    def test_skips_entities_without_fdc(self):
        entities = pd.DataFrame(
            [{"external_ids": {}}],
            index=pd.Index(["e0"], name="foodatlas_id"),
        )
        assert build_fdc_id_map(entities) == {}

    def test_duplicate_raises_value_error(self):
        entities = pd.DataFrame(
            [
                {"external_ids": {"fdc": [100]}},
                {"external_ids": {"fdc": [100]}},
            ],
            index=pd.Index(["e0", "e1"], name="foodatlas_id"),
        )
        with pytest.raises(ValueError, match="Duplicate FDC ID"):
            build_fdc_id_map(entities)


class TestLoadFdcNutrients:
    def test_loads_and_filters(self, tmp_path):
        food_nutrient = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "fdc_id": [100, 200, 300],
                "nutrient_id": [1001, 1002, 1003],
                "amount": [1.5, 2.5, 3.5],
            }
        )
        food_nutrient.to_csv(tmp_path / "food_nutrient.csv", index=False)

        foundation = pd.DataFrame({"fdc_id": [100, 300]})
        foundation.to_csv(tmp_path / "foundation_food.csv", index=False)

        result = load_fdc_nutrients(tmp_path)
        assert len(result) == 2
        assert set(result["fdc_id"]) == {100, 300}


class TestBuildFdcMetadata:
    def test_builds_metadata_rows(self):
        fdc_data = pd.DataFrame(
            {"fdc_id": [100], "nutrient_id": [1001], "amount": [1.5]}
        )
        fdc_nutrients = pd.DataFrame(
            {"id": [1001], "name": ["Vitamin C"], "unit_name": ["MG"]}
        ).set_index("id")
        fdc2fa = {100: "e0"}
        fdcn2fa = {1001: "e1"}

        result = build_fdc_metadata(fdc_data, fdc_nutrients, fdc2fa, fdcn2fa)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["_food_name"] == "FDC:100"
        assert row["_chemical_name"] == "FDC_NUTRIENT:1001"
        assert row["_conc"] == "1.5 mg/100g"
        assert row["source"] == "fdc"
        assert "fdc-app" in row["reference"][0]

    def test_skips_unmapped_ids(self):
        fdc_data = pd.DataFrame(
            {"fdc_id": [999], "nutrient_id": [1001], "amount": [1.0]}
        )
        fdc_nutrients = pd.DataFrame(
            {"id": [1001], "name": ["Vitamin C"], "unit_name": ["MG"]}
        ).set_index("id")

        result = build_fdc_metadata(fdc_data, fdc_nutrients, {}, {})
        assert result.empty

    def test_no_global_state(self):
        fdc_data = pd.DataFrame(
            {"fdc_id": [100, 100], "nutrient_id": [1001, 1002], "amount": [1.0, 2.0]}
        )
        fdc_nutrients = pd.DataFrame(
            {"id": [1001, 1002], "name": ["A", "B"], "unit_name": ["MG", "UG"]}
        ).set_index("id")
        fdc2fa = {100: "e0"}
        fdcn2fa = {1001: "e1", 1002: "e2"}

        result = build_fdc_metadata(fdc_data, fdc_nutrients, fdc2fa, fdcn2fa)
        assert len(result) == 2

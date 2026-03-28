"""Tests for the MetadataContainsStore class."""

import json
from pathlib import Path

import pandas as pd
import pytest
from src.stores.metadata_store import COLUMNS, FAID_PREFIX, MetadataContainsStore
from src.stores.schema import FILE_METADATA_CONTAINS


@pytest.fixture()
def metadata_dir(tmp_path: Path) -> Path:
    data = [
        {
            "foodatlas_id": "mc0",
            "conc_value": 1.5,
            "conc_unit": "mg/g",
            "food_part": "peel",
            "food_processing": "raw",
            "source": "fdc",
            "reference": ["ref1"],
            "entity_linking_method": "exact",
            "quality_score": 0.95,
            "_food_name": "apple",
            "_chemical_name": "vitamin c",
            "_conc": "1.5 mg/g",
            "_food_part": "peel",
        },
    ]
    with (tmp_path / FILE_METADATA_CONTAINS).open("w") as f:
        json.dump(data, f)
    return tmp_path


@pytest.fixture()
def metadata(metadata_dir: Path) -> MetadataContainsStore:
    return MetadataContainsStore(
        path_metadata_contains=metadata_dir / FILE_METADATA_CONTAINS
    )


class TestMetadataContainsStoreLoad:
    def test_loads_metadata(self, metadata: MetadataContainsStore) -> None:
        assert len(metadata._records) == 1

    def test_curr_mcid_increments(self, metadata: MetadataContainsStore) -> None:
        assert metadata._curr_mcid == 1

    def test_faid_prefix(self) -> None:
        assert FAID_PREFIX == "mc"


class TestMetadataContainsStoreCreate:
    def test_creates_with_auto_ids(self, metadata: MetadataContainsStore) -> None:
        new_meta = pd.DataFrame(
            [
                {
                    "conc_value": 2.0,
                    "conc_unit": "mg/g",
                    "food_part": "flesh",
                    "food_processing": "cooked",
                    "source": "fdc",
                    "reference": ["ref2"],
                    "entity_linking_method": "fuzzy",
                    "quality_score": 0.8,
                    "_food_name": "banana",
                    "_chemical_name": "potassium",
                    "_conc": "2.0 mg/g",
                    "_food_part": "flesh",
                }
            ]
        )
        result = metadata.create(new_meta)
        assert "mc1" in result.index
        assert len(metadata._records) == 2

    def test_id_counter_advances(self, metadata: MetadataContainsStore) -> None:
        new_meta = pd.DataFrame(
            [
                {
                    "conc_value": 3.0,
                    "conc_unit": "ug/g",
                    "food_part": "",
                    "food_processing": "",
                    "source": "lit",
                    "reference": ["ref3"],
                    "entity_linking_method": "exact",
                    "quality_score": 0.9,
                    "_food_name": "cherry",
                    "_chemical_name": "anthocyanin",
                    "_conc": "3.0 ug/g",
                    "_food_part": "",
                },
                {
                    "conc_value": 4.0,
                    "conc_unit": "mg/g",
                    "food_part": "",
                    "food_processing": "",
                    "source": "lit",
                    "reference": ["ref4"],
                    "entity_linking_method": "exact",
                    "quality_score": 0.7,
                    "_food_name": "grape",
                    "_chemical_name": "resveratrol",
                    "_conc": "4.0 mg/g",
                    "_food_part": "",
                },
            ]
        )
        metadata.create(new_meta)
        assert metadata._curr_mcid == 3


class TestMetadataContainsStoreGet:
    def test_retrieves_by_ids(self, metadata: MetadataContainsStore) -> None:
        result = metadata.get(["mc0"])
        assert len(result) == 1
        assert result.loc["mc0", "conc_unit"] == "mg/g"


class TestMetadataContainsStoreColumnValidation:
    def test_columns_match_expected(self) -> None:
        expected = [
            "foodatlas_id",
            "conc_value",
            "conc_unit",
            "food_part",
            "food_processing",
            "source",
            "reference",
            "entity_linking_method",
            "quality_score",
            "_food_name",
            "_chemical_name",
            "_conc",
            "_food_part",
        ]
        assert expected == COLUMNS


class TestMetadataContainsStoreSaveReload:
    def test_round_trip(self, metadata: MetadataContainsStore, tmp_path: Path) -> None:
        out_dir = tmp_path / "output"
        out_dir.mkdir()
        metadata.save(out_dir)

        reloaded = MetadataContainsStore(
            path_metadata_contains=out_dir / FILE_METADATA_CONTAINS
        )
        assert len(reloaded._records) == len(metadata._records)
        row = reloaded._records.loc["mc0"]
        assert row["conc_unit"] == "mg/g"
        assert row["reference"] == ["ref1"]

"""Tests for re-keying the whole-food bioactivity file: delta ids become PTFI
sample ids, snapshot foods keep their id, anything else fails."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.ingest.prepare.bioactivity import (
    COPIED_FILES,
    FOOD_FILE,
    prepare_bioactivity,
    rekey_food_heads,
    write_bioactivity,
)

if TYPE_CHECKING:
    from pathlib import Path


def _delta_entities() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "foodatlas_id": "e1",
                "entity_type": "food",
                "external_ids": json.dumps({"ptfi": ["GGB200", "GGB100"]}),
            },
            {
                "foodatlas_id": "e3",
                "entity_type": "chemical",
                "external_ids": json.dumps({"pubchem_compound": [5192]}),
            },
        ]
    )


def _snapshot() -> pd.DataFrame:
    """e1 collides: a food in the delta, a different food in the snapshot."""
    return pd.DataFrame(
        [
            {"foodatlas_id": "e1", "entity_type": "food", "common_name": "milk"},
            {"foodatlas_id": "e30", "entity_type": "food", "common_name": "rice"},
            {"foodatlas_id": "e40", "entity_type": "chemical", "common_name": "x"},
        ]
    ).set_index("foodatlas_id")


def _edges(*ids: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "foodatlas_id": list(ids),
            "bioactivity_id": "E300002",
            "relationship": "exhibits",
            "bioactivity_metadata_ids": '["bm1"]',
        }
    )


class TestRekey:
    def test_delta_food_becomes_its_lowest_sample_id(self) -> None:
        out = rekey_food_heads(_edges("e1"), _delta_entities(), _snapshot())
        assert list(out.columns)[:2] == ["foodatlas_id", "ptfi_sample_id"]
        assert out.iloc[0]["foodatlas_id"] == ""
        assert out.iloc[0]["ptfi_sample_id"] == "GGB100"

    def test_snapshot_food_keeps_its_id(self) -> None:
        out = rekey_food_heads(_edges("e30"), _delta_entities(), _snapshot())
        assert out.iloc[0]["foodatlas_id"] == "e30"
        assert out.iloc[0]["ptfi_sample_id"] == ""

    def test_delta_wins_a_collision_with_a_snapshot_id(self) -> None:
        """The file is written in the delta's id space: e1 means the delta's
        pepper, not the snapshot's milk."""
        out = rekey_food_heads(_edges("e1", "e30"), _delta_entities(), _snapshot())
        assert list(out["ptfi_sample_id"]) == ["GGB100", ""]

    def test_chemical_id_fails(self) -> None:
        with pytest.raises(ValueError, match="e40"):
            rekey_food_heads(_edges("e40"), _delta_entities(), _snapshot())

    def test_unknown_id_fails(self) -> None:
        with pytest.raises(ValueError, match="e999"):
            rekey_food_heads(_edges("e30", "e999"), _delta_entities(), _snapshot())


class TestDeliveryFiles:
    @pytest.fixture
    def delivery(self, tmp_path: Path) -> Path:
        delivery = tmp_path / "delivery"
        delivery.mkdir()
        for name in COPIED_FILES:
            (delivery / name).write_text("header\nrow\n")
        pd.DataFrame({"foodatlas_id": ["e30"], "CID": [5192]}).to_csv(
            delivery / "food_chemical_efficacy.csv", index=False
        )
        _edges("e1", "e30").to_csv(delivery / FOOD_FILE, index=False)
        return delivery

    def test_prepare_rekeys_and_write_copies_the_rest(
        self, delivery: Path, tmp_path: Path
    ) -> None:
        food_edges = prepare_bioactivity(delivery, _delta_entities(), _snapshot())
        out = tmp_path / "data" / "Bioactivity"
        write_bioactivity(delivery, food_edges, out)
        assert {p.name for p in out.iterdir()} == {*COPIED_FILES, FOOD_FILE}
        written = pd.read_csv(out / FOOD_FILE).fillna("")
        assert list(written["ptfi_sample_id"]) == ["GGB100", ""]

    def test_efficacy_citing_a_non_food_fails(self, delivery: Path) -> None:
        pd.DataFrame({"foodatlas_id": ["e40"], "CID": [1]}).to_csv(
            delivery / "food_chemical_efficacy.csv", index=False
        )
        with pytest.raises(ValueError, match=r"food_chemical_efficacy\.csv"):
            prepare_bioactivity(delivery, _delta_entities(), _snapshot())

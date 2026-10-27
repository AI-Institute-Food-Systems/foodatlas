"""End-to-end test of preparing a delivery: files in, both source dirs out,
nothing written when validation fails."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.ingest.prepare import prepare_delivery
from src.pipeline.ingest.prepare.bioactivity import COPIED_FILES, FOOD_FILE
from src.pipeline.ingest.prepare.snapshot import latest_snapshot

from tests.test_prepare_ptfi import _delta, _snapshot

if TYPE_CHECKING:
    from pathlib import Path


def _write_delivery(delivery: Path, delta: dict[str, pd.DataFrame]) -> None:
    (delivery / "ptfi").mkdir(parents=True)
    for name, frame in delta.items():
        frame.to_csv(delivery / "ptfi" / f"{name}_ptfi_delta.csv", index=False)
    for name in COPIED_FILES:
        (delivery / name).write_text("header\nrow\n")
    pd.DataFrame({"foodatlas_id": ["e30"], "CID": [5192]}).to_csv(
        delivery / "food_chemical_efficacy.csv", index=False
    )
    pd.DataFrame(
        {
            "foodatlas_id": ["e1", "e30"],
            "bioactivity_id": "E300002",
            "relationship": "exhibits",
            "bioactivity_metadata_ids": '["bm1"]',
        }
    ).to_csv(delivery / FOOD_FILE, index=False)


def _write_snapshots(previous: Path) -> None:
    """Two snapshots; only the newer holds the entities the delivery cites."""
    (previous / "20260101T000000Z").mkdir(parents=True)
    (previous / "20260101T000000Z" / "entities.parquet").write_bytes(b"")
    newer = previous / "20261008T054749Z"
    newer.mkdir()
    entities = _snapshot().reset_index()
    for column in ("synonyms", "external_ids"):
        entities[column] = entities[column].map(json.dumps)
    entities.to_parquet(newer / "entities.parquet", index=False)


@pytest.fixture
def layout(tmp_path: Path) -> tuple[Path, Path]:
    _write_delivery(tmp_path / "delivery", _delta())
    _write_snapshots(tmp_path / "data" / "PreviousFAKG")
    return tmp_path / "delivery", tmp_path / "data"


def test_latest_snapshot_is_the_newest_with_entities(layout: tuple[Path, Path]) -> None:
    _, data = layout
    assert latest_snapshot(data / "PreviousFAKG").name == "20261008T054749Z"


def test_prepare_writes_both_source_dirs(layout: tuple[Path, Path]) -> None:
    delivery, data = layout
    prepare_delivery(delivery, latest_snapshot(data / "PreviousFAKG"), data)

    ptfi = {p.name for p in (data / "PTFI").iterdir()}
    assert ptfi == {
        "ptfi_foods.csv",
        "ptfi_chemicals.csv",
        "ptfi_measurements.csv",
        "ptfi_evidence.csv",
    }
    measurements = pd.read_csv(data / "PTFI" / "ptfi_measurements.csv")
    assert len(measurements) == 5
    assert set(measurements["chemical_key"]) == {
        "pubchem:5192",
        "pubchem:77",
        "chebi:36080",
        "fdc_nutrient:1007",
    }

    bioactivity = {p.name for p in (data / "Bioactivity").iterdir()}
    assert bioactivity == {*COPIED_FILES, FOOD_FILE}
    food = pd.read_csv(data / "Bioactivity" / FOOD_FILE).fillna("")
    assert list(food["ptfi_sample_id"]) == ["GGB100", ""]
    assert list(food["foodatlas_id"]) == ["", "e30"]


def test_validation_failure_writes_nothing(layout: tuple[Path, Path]) -> None:
    delivery, data = layout
    broken = _delta(triplets=[("e1", "e23", ["at1"])])
    broken["triplets"].to_csv(
        delivery / "ptfi" / "triplets_ptfi_delta.csv", index=False
    )
    with pytest.raises(ValueError, match="e23"):
        prepare_delivery(delivery, latest_snapshot(data / "PreviousFAKG"), data)
    assert not (data / "PTFI").exists()
    assert not (data / "Bioactivity").exists()

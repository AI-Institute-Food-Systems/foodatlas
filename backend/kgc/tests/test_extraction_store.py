"""Tests for ExtractionStore."""

from pathlib import Path

import pandas as pd
import pytest
from src.stores.extraction_store import ExtractionStore, extraction_id
from src.stores.schema import EXTRACTION_COLUMNS, FILE_EXTRACTIONS


@pytest.fixture()
def store(tmp_path: Path) -> ExtractionStore:
    pd.DataFrame(columns=EXTRACTION_COLUMNS).to_parquet(
        tmp_path / FILE_EXTRACTIONS, index=False
    )
    return ExtractionStore(path=tmp_path / FILE_EXTRACTIONS)


def _make_row(**overrides: float | str | bool | None) -> dict:
    base = {
        "evidence_id": "ev_test",
        "extractor": "lit2kg:gpt-3.5-ft",
        "head_name_raw": "apple",
        "tail_name_raw": "vitamin c",
        "conc_value": None,
        "conc_unit": "",
        "food_part": "",
        "food_processing": "",
        "quality_score": 0.95,
        "validated": False,
        "validated_correct": True,
    }
    base.update(overrides)
    return base


class TestExtractionId:
    def test_deterministic(self) -> None:
        id1 = extraction_id("ev1", "model_a", "apple", "vc")
        id2 = extraction_id("ev1", "model_a", "apple", "vc")
        assert id1 == id2

    def test_different_extractor(self) -> None:
        id1 = extraction_id("ev1", "model_a", "apple", "vc")
        id2 = extraction_id("ev1", "model_b", "apple", "vc")
        assert id1 != id2

    def test_prefix(self) -> None:
        eid = extraction_id("ev1", "m", "a", "b")
        assert eid.startswith("ex")


class TestExtractionStoreCreate:
    def test_creates_records(self, store: ExtractionStore) -> None:
        rows = pd.DataFrame([_make_row()])
        result = store.create(rows)
        assert len(result) == 1
        assert result.index[0].startswith("ex")

    def test_deterministic_ids(self, store: ExtractionStore) -> None:
        row = _make_row()
        r1 = store.create(pd.DataFrame([row]))
        r2 = store.create(pd.DataFrame([row]))
        assert r1.index[0] == r2.index[0]


class TestExtractionStoreGet:
    def test_retrieves_by_id(self, store: ExtractionStore) -> None:
        rows = pd.DataFrame([_make_row()])
        result = store.create(rows)
        ex_id = result.index[0]

        retrieved = store.get([ex_id])
        assert len(retrieved) == 1
        assert retrieved.loc[ex_id, "extractor"] == "lit2kg:gpt-3.5-ft"

    def test_missing_id_returns_empty(self, store: ExtractionStore) -> None:
        result = store.get(["nonexistent"])
        assert len(result) == 0


class TestExtractionStoreSaveReload:
    def test_round_trip(self, store: ExtractionStore, tmp_path: Path) -> None:
        store.create(pd.DataFrame([_make_row()]))

        out = tmp_path / "out"
        out.mkdir()
        store.save(out)

        reloaded = ExtractionStore(path=out / FILE_EXTRACTIONS)
        assert len(reloaded) == 1

"""Tests for AttestationStore."""

from pathlib import Path

import pandas as pd
import pytest
from src.stores.attestation_store import AttestationStore, attestation_id
from src.stores.schema import ATTESTATION_COLUMNS, FILE_ATTESTATIONS


@pytest.fixture()
def store(tmp_path: Path) -> AttestationStore:
    pd.DataFrame(columns=ATTESTATION_COLUMNS).to_parquet(
        tmp_path / FILE_ATTESTATIONS, index=False
    )
    return AttestationStore(path=tmp_path / FILE_ATTESTATIONS)


def _make_row(**overrides: float | str | bool | None) -> dict:
    base = {
        "evidence_id": "ev_test",
        "source": "lit2kg:gpt-3.5-ft",
        "head_name_raw": "apple",
        "tail_name_raw": "vitamin c",
        "conc_value": None,
        "conc_unit": "",
        "conc_value_raw": "",
        "conc_unit_raw": "",
        "food_part": "",
        "food_processing": "",
        "filter_score": 0.95,
        "validated": False,
        "validated_correct": True,
    }
    base.update(overrides)
    return base


class TestAttestationId:
    def test_deterministic(self) -> None:
        id1 = attestation_id("ev1", "model_a", "apple", "vc")
        id2 = attestation_id("ev1", "model_a", "apple", "vc")
        assert id1 == id2

    def test_different_source(self) -> None:
        id1 = attestation_id("ev1", "model_a", "apple", "vc")
        id2 = attestation_id("ev1", "model_b", "apple", "vc")
        assert id1 != id2

    def test_prefix(self) -> None:
        eid = attestation_id("ev1", "m", "a", "b")
        assert eid.startswith("at")


class TestAttestationStoreCreate:
    def test_creates_records(self, store: AttestationStore) -> None:
        rows = pd.DataFrame([_make_row()])
        result = store.create(rows)
        assert len(result) == 1
        assert result.index[0].startswith("at")

    def test_deterministic_ids(self, store: AttestationStore) -> None:
        row = _make_row()
        r1 = store.create(pd.DataFrame([row]))
        r2 = store.create(pd.DataFrame([row]))
        assert r1.index[0] == r2.index[0]


class TestAttestationStoreGet:
    def test_retrieves_by_id(self, store: AttestationStore) -> None:
        rows = pd.DataFrame([_make_row()])
        result = store.create(rows)
        att_id = result.index[0]

        retrieved = store.get([att_id])
        assert len(retrieved) == 1
        assert retrieved.loc[att_id, "source"] == "lit2kg:gpt-3.5-ft"

    def test_missing_id_returns_empty(self, store: AttestationStore) -> None:
        result = store.get(["nonexistent"])
        assert len(result) == 0


class TestAttestationStoreSaveReload:
    def test_round_trip(self, store: AttestationStore, tmp_path: Path) -> None:
        store.create(pd.DataFrame([_make_row()]))

        out = tmp_path / "out"
        out.mkdir()
        store.save(out)

        reloaded = AttestationStore(path=out / FILE_ATTESTATIONS)
        assert len(reloaded) == 1

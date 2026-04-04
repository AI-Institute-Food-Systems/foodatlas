"""Tests for EvidenceStore."""

from pathlib import Path

import pandas as pd
import pytest
from src.stores.evidence_store import EvidenceStore, evidence_id
from src.stores.schema import EVIDENCE_COLUMNS, FILE_EVIDENCE


@pytest.fixture()
def store(tmp_path: Path) -> EvidenceStore:
    pd.DataFrame(columns=EVIDENCE_COLUMNS).to_parquet(
        tmp_path / FILE_EVIDENCE, index=False
    )
    return EvidenceStore(path=tmp_path / FILE_EVIDENCE)


class TestEvidenceId:
    def test_deterministic(self) -> None:
        id1 = evidence_id("pubmed", '{"pmcid": 123}')
        id2 = evidence_id("pubmed", '{"pmcid": 123}')
        assert id1 == id2

    def test_different_content(self) -> None:
        id1 = evidence_id("pubmed", '{"pmcid": 123}')
        id2 = evidence_id("pubmed", '{"pmcid": 456}')
        assert id1 != id2

    def test_prefix(self) -> None:
        eid = evidence_id("fdc", '{"url": "x"}')
        assert eid.startswith("ev")


class TestEvidenceStoreCreate:
    def test_creates_records(self, store: EvidenceStore) -> None:
        rows = pd.DataFrame([{"source_type": "pubmed", "reference": '{"pmcid": 1}'}])
        result = store.create(rows)
        assert len(result) == 1
        assert len(store) == 1

    def test_deduplicates(self, store: EvidenceStore) -> None:
        rows = pd.DataFrame(
            [
                {"source_type": "pubmed", "reference": '{"pmcid": 1}'},
                {"source_type": "pubmed", "reference": '{"pmcid": 1}'},
            ]
        )
        store.create(rows)
        assert len(store) == 1

    def test_second_create_skips_existing(self, store: EvidenceStore) -> None:
        rows = pd.DataFrame([{"source_type": "pubmed", "reference": '{"pmcid": 1}'}])
        store.create(rows)
        store.create(rows)
        assert len(store) == 1


class TestEvidenceStoreSaveReload:
    def test_round_trip(self, store: EvidenceStore, tmp_path: Path) -> None:
        rows = pd.DataFrame([{"source_type": "fdc", "reference": '{"url": "test"}'}])
        store.create(rows)

        out = tmp_path / "out"
        out.mkdir()
        store.save(out)

        reloaded = EvidenceStore(path=out / FILE_EVIDENCE)
        assert len(reloaded) == 1

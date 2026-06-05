"""Tests for report.load_old — loading KG parquet snapshots."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.report.load_old import OldKG, load_old_kg

if TYPE_CHECKING:
    from pathlib import Path


def _write_snapshot(base: Path) -> Path:
    """Write a minimal parquet snapshot under base/PreviousFAKG/<ts>/."""
    snapshot = base / "PreviousFAKG" / "20260101T000000Z"
    snapshot.mkdir(parents=True)

    entities = pd.DataFrame(
        {
            "foodatlas_id": ["e1", "e2"],
            "entity_type": ["food", "chemical"],
            "common_name": ["apple", "water"],
            "scientific_name": [None, None],
        }
    )
    entities.to_parquet(snapshot / "entities.parquet", index=False)

    triplets = pd.DataFrame(
        {
            "head_id": ["e1", "e3"],
            "relationship_id": ["r1", "r2"],
            "tail_id": ["e2", "e4"],
        }
    )
    triplets.to_parquet(snapshot / "triplets.parquet", index=False)

    attestations = pd.DataFrame({"source": ["fdc", "fdc", "dmd", "ctd"]})
    attestations.to_parquet(snapshot / "attestations.parquet", index=False)

    return snapshot


class TestLoadOldKG:
    """Tests for load_old_kg and related helpers."""

    def test_integration(self, tmp_path: Path) -> None:
        _write_snapshot(tmp_path)
        kg = load_old_kg(str(tmp_path))

        assert isinstance(kg, OldKG)
        assert len(kg.entities) == 2
        assert kg.entities.index.name == "foodatlas_id"
        assert len(kg.triplets) == 2

    def test_contains_sources(self, tmp_path: Path) -> None:
        _write_snapshot(tmp_path)
        kg = load_old_kg(str(tmp_path))

        assert kg.metadata_contains_sources["fdc"] == 2
        assert kg.metadata_contains_sources["dmd"] == 1
        assert "ctd" not in kg.metadata_contains_sources

    def test_diseases_sources(self, tmp_path: Path) -> None:
        _write_snapshot(tmp_path)
        kg = load_old_kg(str(tmp_path))

        assert kg.metadata_diseases_sources["ctd"] == 1
        assert "fdc" not in kg.metadata_diseases_sources

    def test_picks_latest_snapshot(self, tmp_path: Path) -> None:
        _write_snapshot(tmp_path)

        newer = tmp_path / "PreviousFAKG" / "20260601T000000Z"
        newer.mkdir(parents=True)
        pd.DataFrame(
            {
                "foodatlas_id": ["e1"],
                "entity_type": ["food"],
                "common_name": ["x"],
                "scientific_name": [None],
            }
        ).to_parquet(newer / "entities.parquet", index=False)
        pd.DataFrame({"head_id": [], "relationship_id": [], "tail_id": []}).to_parquet(
            newer / "triplets.parquet", index=False
        )
        pd.DataFrame({"source": pd.Series([], dtype=str)}).to_parquet(
            newer / "attestations.parquet", index=False
        )

        kg = load_old_kg(str(tmp_path))
        assert len(kg.entities) == 1

    def test_no_snapshots_raises(self, tmp_path: Path) -> None:
        (tmp_path / "PreviousFAKG").mkdir()
        with pytest.raises(FileNotFoundError):
            load_old_kg(str(tmp_path))

"""Tests for the bioactivity triplets/promote stages: measurements pruning
and bioassays pruning. Both run after the triplets stage; here we mock the
ingest parquet inputs and a minimal KnowledgeGraph."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.pipeline.triplets.bioactivity.bioassays import promote_bioassays
from src.pipeline.triplets.bioactivity.measurements import (
    promote_bioactivity_measurements,
)


@pytest.fixture
def settings(tmp_path: Path) -> SimpleNamespace:
    ingest = tmp_path / "ingest"
    kg = tmp_path / "kg"
    (ingest / "bioactivity").mkdir(parents=True)
    kg.mkdir()
    return SimpleNamespace(ingest_dir=ingest, kg_dir=kg)


def _make_kg(triplets: pd.DataFrame) -> MagicMock:
    """Build a KnowledgeGraph stub whose .triplets._triplets is a DataFrame."""
    kg = MagicMock()
    kg.triplets = MagicMock()
    kg.triplets._triplets = triplets
    return kg


class TestPromoteMeasurements:
    def test_prunes_to_referenced_ids(self, settings: SimpleNamespace) -> None:
        # Write a 3-row source; only bm1 and bm3 are referenced by triplets.
        src = settings.ingest_dir / "bioactivity" / "bioactivity_measurements.parquet"
        pd.DataFrame(
            {
                "bioactivity_metadata_id": ["bm1", "bm2", "bm3"],
                "potency_value": [1.0, 2.0, 3.0],
            }
        ).to_parquet(src, index=False)

        kg = _make_kg(
            pd.DataFrame(
                {
                    "relationship_id": ["r5", "r6", "r1"],
                    "attestation_ids": [["bm1"], ["bm3"], ["bm2"]],
                }
            )
        )
        promote_bioactivity_measurements(settings, kg)

        out = pd.read_parquet(settings.kg_dir / "attestations_bioactivity.parquet")
        assert set(out["bioactivity_metadata_id"]) == {"bm1", "bm3"}

    def test_noop_when_source_missing(self, settings: SimpleNamespace) -> None:
        promote_bioactivity_measurements(settings, _make_kg(pd.DataFrame()))
        assert not (settings.kg_dir / "attestations_bioactivity.parquet").exists()


class TestPromoteBioassays:
    def test_prunes_to_referenced_assays(self, settings: SimpleNamespace) -> None:
        # Write the full assay table — only A1 is referenced by measurements.
        pd.DataFrame(
            {"source_assay_id": ["A1", "A2", "A3"], "source": ["x", "y", "z"]}
        ).to_parquet(
            settings.ingest_dir / "bioactivity" / "bioactivity_bioassays.parquet",
            index=False,
        )
        pd.DataFrame(
            {"source_assay_id": ["A1", "A1"], "bioactivity_metadata_id": ["bm1", "bm2"]}
        ).to_parquet(
            settings.kg_dir / "attestations_bioactivity.parquet", index=False
        )
        promote_bioassays(settings)
        out = pd.read_parquet(settings.kg_dir / "bioassays.parquet")
        assert list(out["source_assay_id"]) == ["A1"]

    def test_noop_when_inputs_missing(self, settings: SimpleNamespace) -> None:
        promote_bioassays(settings)
        assert not (settings.kg_dir / "bioassays.parquet").exists()

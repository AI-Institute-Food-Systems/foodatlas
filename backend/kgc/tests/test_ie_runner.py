"""Tests for IERunner."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from src.models.settings import KGCSettings
from src.pipeline.ie.resolver import IEResolutionResult
from src.pipeline.ie.runner import IERunner


@pytest.fixture
def settings(tmp_path: Path) -> KGCSettings:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir()
    s = KGCSettings(
        kg_dir=str(kg_dir),
        data_dir=str(tmp_path / "data"),
        output_dir=str(tmp_path / "out"),
        cache_dir=str(tmp_path / "cache"),
    )
    s.pipeline.stages.triplet_expansion.ie_raw_paths = {}
    return s


@patch("src.pipeline.ie.runner.KnowledgeGraph")
def test_run_skips_expansion_when_no_ie_path(
    mock_kg_cls: MagicMock,
    settings: KGCSettings,
) -> None:
    kg = MagicMock()
    mock_kg_cls.return_value = kg

    runner = IERunner(settings)
    runner.run()

    kg.add_triplets_from_resolved_ie.assert_not_called()
    kg.save.assert_called_once()


@patch("src.pipeline.ie.runner.write_unresolved_report")
@patch("src.pipeline.ie.runner.resolve_ie_metadata")
@patch("src.pipeline.ie.runner.load_ie_raw")
@patch("src.pipeline.ie.runner.KnowledgeGraph")
def test_ie_expansion_calls_resolver(
    mock_kg_cls: MagicMock,
    mock_load_ie: MagicMock,
    mock_resolve: MagicMock,
    mock_write_report: MagicMock,
    tmp_path: Path,
) -> None:
    ie_path = tmp_path / "ie.tsv"
    ie_path.write_text("dummy")

    settings = KGCSettings(
        kg_dir=str(tmp_path / "kg"),
        data_dir=str(tmp_path / "data"),
        output_dir=str(tmp_path / "out"),
        cache_dir=str(tmp_path / "cache"),
    )
    (tmp_path / "kg").mkdir()
    settings.pipeline.stages.triplet_expansion.ie_raw_paths = {"gpt-4": str(ie_path)}

    kg = MagicMock()
    mock_kg_cls.return_value = kg

    mock_load_ie.return_value = pd.DataFrame(
        [{"_food_name": "apple", "_chemical_name": "vitamin c"}]
    )

    mock_resolve.return_value = IEResolutionResult(
        resolved=pd.DataFrame([{"head_id": "e0", "tail_id": "e1"}]),
        unresolved_food=set(),
        unresolved_chemical=set(),
        stats={"total_ie_rows": 1, "resolved_rows": 1},
    )

    runner = IERunner(settings)
    runner.run()

    mock_load_ie.assert_called_once()
    mock_resolve.assert_called_once()
    kg.add_triplets_from_resolved_ie.assert_called_once()
    mock_write_report.assert_called_once()

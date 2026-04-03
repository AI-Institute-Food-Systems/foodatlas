"""Tests for TripletRunner."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from src.models.settings import KGCSettings
from src.pipeline.triplets.ambiguity import AmbiguityReport
from src.pipeline.triplets.runner import TripletRunner


@pytest.fixture
def settings(tmp_path: Path) -> KGCSettings:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir()
    return KGCSettings(
        kg_dir=str(kg_dir),
        data_dir=str(tmp_path / "data"),
        output_dir=str(tmp_path / "out"),
        cache_dir=str(tmp_path / "cache"),
    )


@patch("src.pipeline.triplets.runner.load_sources")
@patch("src.pipeline.triplets.runner.create_empty_triplet_files")
@patch("src.pipeline.triplets.runner.KnowledgeGraph")
@patch("src.pipeline.triplets.runner.build_triplets")
def test_run_calls_build_and_save(
    mock_build: MagicMock,
    mock_kg_cls: MagicMock,
    mock_scaffold: MagicMock,
    mock_loader: MagicMock,
    settings: KGCSettings,
) -> None:
    sources = {"fdc": {"edges": MagicMock()}}
    mock_loader.return_value = sources
    mock_build.return_value = AmbiguityReport()
    kg = MagicMock()
    mock_kg_cls.return_value = kg

    runner = TripletRunner(settings)
    runner.run()

    mock_loader.assert_called_once_with(settings)
    mock_scaffold.assert_called_once_with(settings)
    mock_build.assert_called_once_with(kg, sources)
    kg.save.assert_called_once()

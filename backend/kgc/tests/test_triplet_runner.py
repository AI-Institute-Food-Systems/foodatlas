"""Tests for TripletRunner."""

import json
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from src.models.settings import KGCSettings
from src.pipeline.triplets.ie_resolver import IEResolutionResult
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


@patch("src.pipeline.triplets.runner.load_ingest_output")
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
    kg = MagicMock()
    mock_kg_cls.return_value = kg

    runner = TripletRunner(settings)
    runner.run()

    mock_loader.assert_called_once_with(settings)
    mock_scaffold.assert_called_once_with(settings)
    mock_build.assert_called_once_with(kg, sources)
    kg.save.assert_called_once()


@patch("src.pipeline.triplets.runner.load_ingest_output")
@patch("src.pipeline.triplets.runner.create_empty_triplet_files")
@patch("src.pipeline.triplets.runner.KnowledgeGraph")
@patch("src.pipeline.triplets.runner.build_triplets")
def test_run_skips_expansion_when_no_ie_path(
    mock_build: MagicMock,
    mock_kg_cls: MagicMock,
    mock_scaffold: MagicMock,
    mock_loader: MagicMock,
    settings: KGCSettings,
) -> None:
    mock_loader.return_value = {}
    kg = MagicMock()
    mock_kg_cls.return_value = kg

    runner = TripletRunner(settings)
    runner.run()

    kg.add_triplets_from_metadata.assert_not_called()
    kg.add_triplets_from_resolved_ie.assert_not_called()
    kg.save.assert_called_once()


@patch("src.pipeline.triplets.runner.load_ingest_output")
@patch("src.pipeline.triplets.runner.create_empty_triplet_files")
@patch("src.pipeline.triplets.runner.KnowledgeGraph")
@patch("src.pipeline.triplets.runner.build_triplets")
def test_legacy_metadata_json_triggers_deprecation(
    mock_build: MagicMock,
    mock_kg_cls: MagicMock,
    mock_scaffold: MagicMock,
    mock_loader: MagicMock,
    settings: KGCSettings,
) -> None:
    mock_loader.return_value = {}
    kg = MagicMock()
    mock_kg_cls.return_value = kg

    metadata_path = Path(settings.kg_dir) / "_metadata_new.json"
    metadata = [{"_food_name": "apple", "_chemical_name": "vitamin c"}]
    with metadata_path.open("w") as f:
        json.dump(metadata, f)

    runner = TripletRunner(settings)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        runner.run()
        deprecation_warnings = [
            x for x in w if issubclass(x.category, DeprecationWarning)
        ]
        assert len(deprecation_warnings) == 1
        assert "ie_raw_path" in str(deprecation_warnings[0].message)

    kg.add_triplets_from_metadata.assert_called_once()


@patch("src.pipeline.triplets.runner.write_resolution_stats")
@patch("src.pipeline.triplets.runner.write_unresolved_report")
@patch("src.pipeline.triplets.runner.resolve_ie_metadata")
@patch("src.pipeline.triplets.runner.load_ie_raw")
@patch("src.pipeline.triplets.runner.load_ingest_output")
@patch("src.pipeline.triplets.runner.create_empty_triplet_files")
@patch("src.pipeline.triplets.runner.KnowledgeGraph")
@patch("src.pipeline.triplets.runner.build_triplets")
def test_ie_expansion_calls_resolver(
    mock_build: MagicMock,
    mock_kg_cls: MagicMock,
    mock_scaffold: MagicMock,
    mock_loader: MagicMock,
    mock_load_ie: MagicMock,
    mock_resolve: MagicMock,
    mock_write_report: MagicMock,
    mock_write_stats: MagicMock,
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
    settings.pipeline.stages.triplet_expansion.ie_raw_path = str(ie_path)

    mock_loader.return_value = {}
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

    runner = TripletRunner(settings)
    runner.run()

    mock_load_ie.assert_called_once()
    mock_resolve.assert_called_once()
    kg.add_triplets_from_resolved_ie.assert_called_once()
    mock_write_report.assert_called_once()
    mock_write_stats.assert_called_once()

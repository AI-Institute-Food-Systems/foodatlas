"""Tests for IERunner."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from src.models.settings import KGCSettings
from src.pipeline.ie.resolver import IEResolutionResult
from src.pipeline.ie.runner import IERunner


def _make_extraction_dir(
    base: Path,
    name: str,
    *,
    model: str | None = None,
) -> Path:
    """Create a fake extraction subdirectory with JSON and run_info."""
    d = base / name
    d.mkdir(parents=True, exist_ok=True)
    data = {
        "0": {
            "pmcid": 123,
            "section": "INTRO",
            "matched_query": "apple",
            "text": "Apples contain quercetin.",
            "prob": 0.99,
            "response": "(apple, , quercetin, )",
            "triplets": [["apple", "", "quercetin", ""]],
        }
    }
    (d / "extraction_predicted.json").write_text(
        json.dumps(data, indent=2),
    )
    if model is not None:
        (d / "run_info.json").write_text(
            json.dumps({"model": model, "date": name}) + "\n"
        )
    return d


@pytest.fixture
def settings(tmp_path: Path) -> KGCSettings:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir()
    return KGCSettings(
        kg_dir=str(kg_dir),
        data_dir=str(tmp_path / "data"),
        output_dir=str(tmp_path / "out"),
        cache_dir=str(tmp_path / "cache"),
        ie_raw_dir=str(tmp_path / "no_ie"),
    )


class TestDiscoverIEFiles:
    def test_empty_ie_raw_dir_returns_empty(self, settings: KGCSettings) -> None:
        runner = IERunner(settings)
        assert runner._discover_ie_files() == []

    def test_missing_dir_returns_empty(
        self, settings: KGCSettings, tmp_path: Path
    ) -> None:
        settings.ie_raw_dir = str(tmp_path / "nonexistent")
        runner = IERunner(settings)
        assert runner._discover_ie_files() == []

    def test_discovers_files_with_run_info(
        self, settings: KGCSettings, tmp_path: Path
    ) -> None:
        ie_dir = tmp_path / "extractions"
        _make_extraction_dir(ie_dir, "2024_01_01", model="gpt-4")
        _make_extraction_dir(ie_dir, "2024_02_01", model="gpt-5.2")

        settings.ie_raw_dir = str(ie_dir)
        runner = IERunner(settings)
        entries = runner._discover_ie_files()

        assert len(entries) == 2
        assert entries[0] == (
            "gpt-4",
            ie_dir / "2024_01_01" / "extraction_predicted.json",
        )
        assert entries[1] == (
            "gpt-5.2",
            ie_dir / "2024_02_01" / "extraction_predicted.json",
        )

    def test_falls_back_to_dir_name_without_run_info(
        self, settings: KGCSettings, tmp_path: Path
    ) -> None:
        ie_dir = tmp_path / "extractions"
        _make_extraction_dir(ie_dir, "2024_03_15", model=None)

        settings.ie_raw_dir = str(ie_dir)
        runner = IERunner(settings)
        entries = runner._discover_ie_files()

        assert len(entries) == 1
        assert entries[0][0] == "2024_03_15"

    def test_same_model_multiple_dirs(
        self, settings: KGCSettings, tmp_path: Path
    ) -> None:
        ie_dir = tmp_path / "extractions"
        _make_extraction_dir(ie_dir, "2024_01_01", model="gpt-5.2")
        _make_extraction_dir(ie_dir, "2024_02_01", model="gpt-5.2")
        _make_extraction_dir(ie_dir, "2024_03_01", model="gpt-5.2")

        settings.ie_raw_dir = str(ie_dir)
        runner = IERunner(settings)
        entries = runner._discover_ie_files()

        assert len(entries) == 3
        assert all(m == "gpt-5.2" for m, _ in entries)


@patch("src.pipeline.ie.runner.write_unclassified_jsonl")
@patch("src.pipeline.ie.runner.write_orphans_jsonl")
@patch("src.pipeline.ie.runner.KnowledgeGraph")
def test_run_skips_expansion_when_no_ie_dir(
    mock_kg_cls: MagicMock,
    _mock_orphans: MagicMock,
    _mock_unclassified: MagicMock,
    settings: KGCSettings,
) -> None:
    kg = MagicMock()
    mock_kg_cls.return_value = kg

    runner = IERunner(settings)
    runner.run()

    kg.add_triplets_from_resolved_ie.assert_not_called()
    kg.save.assert_called_once()


@patch("src.pipeline.ie.runner.write_unclassified_jsonl")
@patch("src.pipeline.ie.runner.write_orphans_jsonl")
@patch("src.pipeline.ie.runner.write_unresolved_report")
@patch("src.pipeline.ie.runner.resolve_ie_metadata")
@patch("src.pipeline.ie.runner.load_ie_raw")
@patch("src.pipeline.ie.runner.KnowledgeGraph")
def test_ie_expansion_calls_resolver(
    mock_kg_cls: MagicMock,
    mock_load_ie: MagicMock,
    mock_resolve: MagicMock,
    mock_write_report: MagicMock,
    _mock_orphans: MagicMock,
    _mock_unclassified: MagicMock,
    tmp_path: Path,
) -> None:
    ie_dir = tmp_path / "extractions"
    _make_extraction_dir(ie_dir, "2024_01_01", model="gpt-4")

    kg_dir = tmp_path / "kg"
    kg_dir.mkdir()
    settings = KGCSettings(
        kg_dir=str(kg_dir),
        data_dir=str(tmp_path / "data"),
        output_dir=str(tmp_path / "out"),
        cache_dir=str(tmp_path / "cache"),
        ie_raw_dir=str(ie_dir),
    )

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
    call_args = mock_load_ie.call_args
    assert call_args.kwargs["method"] == "gpt-4"
    mock_resolve.assert_called_once()
    kg.add_triplets_from_resolved_ie.assert_called_once()
    mock_write_report.assert_called_once()

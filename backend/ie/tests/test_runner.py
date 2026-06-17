"""Tests for the IE pipeline runner and CLI."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

if TYPE_CHECKING:
    from pathlib import Path

import pytest
from click.testing import CliRunner
from main import cli
from src.models.settings import IESettings
from src.pipeline.runner import IERunner
from src.pipeline.stages import ALL_STAGES, IEStage


def _make_settings(**overrides: str) -> IESettings:
    return IESettings.model_validate(
        {
            "date": "2026_04_06",
            "model": "gpt-4",
            "bioc_pmc_dir": "/tmp/bioc",
            "biobert_model_dir": "outputs/biobert",
            "food_terms": "data/food_terms.txt",
            "translated_food_terms": "data/translated_food_terms.txt",
            **overrides,
        }
    )


class TestIEStage:
    def test_stage_count(self) -> None:
        assert len(IEStage) == 4

    def test_all_stages_sorted(self) -> None:
        values = [s.value for s in ALL_STAGES]
        assert values == [1, 2, 3, 4]

    def test_stage_names(self) -> None:
        assert IEStage.SEARCH.value == 1
        assert IEStage.EXTRACTION.value == 4


class TestIESettings:
    def test_defaults_loaded(self) -> None:
        settings = IESettings.model_validate({})
        assert settings.model == "gpt-5.2"
        assert settings.pipeline.aggregate.threshold == 0.99

    def test_override(self) -> None:
        settings = IESettings.model_validate({"model": "gpt-4"})
        assert settings.model == "gpt-4"

    def test_resolved_date_explicit(self) -> None:
        settings = _make_settings(date="2026_04_06")
        assert settings.resolved_date == "2026_04_06"

    def test_threshold_property(self) -> None:
        settings = _make_settings()
        assert settings.threshold == 0.99

    def test_extraction_prompt_paths(self) -> None:
        settings = _make_settings()
        assert "system" in settings.pipeline.extraction.system_prompt
        assert "user" in settings.pipeline.extraction.user_prompt


class TestIERunner:
    @pytest.fixture()
    def settings(self) -> IESettings:
        return _make_settings()

    @pytest.fixture()
    def runner(self, settings: IESettings, tmp_path: Path) -> IERunner:
        r = IERunner(settings)
        r._pipeline_dir = tmp_path
        # Create dirs that downstream stages expect from prior stages
        date = settings.resolved_date
        search_dir = tmp_path / "outputs" / "search" / date
        search_dir.mkdir(parents=True)
        (search_dir / "query_uid_results.tsv").touch()
        retrieval_dir = tmp_path / "outputs" / "retrieval" / date
        retrieval_dir.mkdir(parents=True)
        (retrieval_dir / "sentence_filtering_input.tsv").touch()
        filter_dir = tmp_path / "outputs" / "filtering" / date
        (filter_dir / "filtered_sentences").mkdir(parents=True)
        (filter_dir / "filtered_sentences" / "information_extraction_input.tsv").touch()
        (tmp_path / "outputs" / "extraction").mkdir(parents=True)
        return r

    def test_run_calls_stages_in_order(self, runner: IERunner) -> None:
        called: list[str] = []

        with (
            patch.object(runner, "_run_search", lambda: called.append("search")),
            patch.object(runner, "_run_retrieval", lambda: called.append("retrieval")),
            patch.object(runner, "_run_filtering", lambda: called.append("filtering")),
            patch.object(
                runner, "_run_extraction", lambda: called.append("extraction")
            ),
        ):
            runner.run([IEStage.SEARCH, IEStage.FILTERING])

        assert called == ["search", "filtering"]

    def test_run_all_stages(self, runner: IERunner) -> None:
        called: list[str] = []

        with (
            patch.object(runner, "_run_search", lambda: called.append("search")),
            patch.object(runner, "_run_retrieval", lambda: called.append("retrieval")),
            patch.object(runner, "_run_filtering", lambda: called.append("filtering")),
            patch.object(
                runner, "_run_extraction", lambda: called.append("extraction")
            ),
        ):
            runner.run()

        assert called == ["search", "retrieval", "filtering", "extraction"]

    @patch("src.pipeline.runner.subprocess.run")
    def test_refresh_pmc_ids_csv_downloads(
        self, mock_run: MagicMock, runner: IERunner
    ) -> None:
        runner._refresh_pmc_ids_csv()

        assert mock_run.call_count == 2
        assert "wget" in mock_run.call_args_list[0][0][0]
        assert "gunzip" in mock_run.call_args_list[1][0][0]

    @patch("src.pipeline.runner.aggregate_food_chem_sentences")
    @patch("src.pipeline.runner.run_biobert_filter")
    def test_filtering_runs_biobert_and_aggregate(
        self,
        mock_filter: MagicMock,
        mock_agg: MagicMock,
        runner: IERunner,
    ) -> None:
        runner._run_filtering()
        mock_filter.assert_called_once()
        mock_agg.assert_called_once()

    @patch("src.pipeline.runner.tsv_to_json")
    @patch("src.pipeline.runner.aggregate_batch_predictions")
    @patch("src.pipeline.runner.run_extraction")
    def test_extraction_runs_extract_and_parse(
        self,
        mock_extract: MagicMock,
        mock_agg: MagicMock,
        mock_json: MagicMock,
        runner: IERunner,
    ) -> None:
        runner._run_extraction()
        mock_extract.assert_called_once()
        mock_agg.assert_called_once()
        mock_json.assert_called_once()

    @patch("src.pipeline.runner.tsv_to_json")
    @patch("src.pipeline.runner.aggregate_batch_predictions")
    @patch("src.pipeline.runner.run_extraction")
    def test_extraction_writes_run_info(
        self,
        mock_extract: MagicMock,
        mock_agg: MagicMock,
        mock_json: MagicMock,
        runner: IERunner,
        settings: IESettings,
        tmp_path: Path,
    ) -> None:
        runner._run_extraction()
        extract_dir = tmp_path / "outputs" / "extraction"
        run_info_path = extract_dir / settings.resolved_date / "run_info.json"
        assert run_info_path.exists()
        info = json.loads(run_info_path.read_text())
        assert info["model"] == "gpt-4"
        assert info["date"] == "2026_04_06"


class TestCLI:
    def test_stages_command(self) -> None:
        result = CliRunner().invoke(cli, ["stages"])
        assert result.exit_code == 0
        assert "SEARCH" in result.output
        assert "RETRIEVAL" in result.output
        assert "EXTRACTION" in result.output

    def test_run_help(self) -> None:
        result = CliRunner().invoke(cli, ["run", "--help"])
        assert result.exit_code == 0
        assert "--stages" in result.output

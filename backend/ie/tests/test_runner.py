"""Tests for the IE pipeline runner and CLI."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
        assert values == list(range(4))

    def test_stage_names(self) -> None:
        assert IEStage.CORPUS.value == 0
        assert IEStage.EXTRACTION.value == 3


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

    def test_prompt_version_property(self) -> None:
        settings = _make_settings()
        assert settings.prompt_version == "v1"


class TestIERunner:
    @pytest.fixture()
    def settings(self) -> IESettings:
        return _make_settings()

    def test_run_calls_stages_in_order(self, settings: IESettings) -> None:
        runner = IERunner(settings)
        called: list[str] = []

        with (
            patch.object(runner, "_run_corpus", lambda: called.append("corpus")),
            patch.object(runner, "_run_search", lambda: called.append("search")),
            patch.object(runner, "_run_filtering", lambda: called.append("filtering")),
            patch.object(
                runner, "_run_extraction", lambda: called.append("extraction")
            ),
        ):
            runner.run([IEStage.SEARCH, IEStage.FILTERING])

        assert called == ["search", "filtering"]

    def test_run_all_stages(self, settings: IESettings) -> None:
        runner = IERunner(settings)
        called: list[str] = []

        with (
            patch.object(runner, "_run_corpus", lambda: called.append("corpus")),
            patch.object(runner, "_run_search", lambda: called.append("search")),
            patch.object(runner, "_run_filtering", lambda: called.append("filtering")),
            patch.object(
                runner, "_run_extraction", lambda: called.append("extraction")
            ),
        ):
            runner.run()

        assert called == ["corpus", "search", "filtering", "extraction"]

    @patch("src.pipeline.runner.update_bioc")
    @patch("src.pipeline.runner.subprocess.run")
    def test_corpus_downloads_and_updates(
        self, mock_run: MagicMock, mock_update: MagicMock, settings: IESettings
    ) -> None:
        runner = IERunner(settings)
        runner._run_corpus()

        assert mock_run.call_count == 2
        assert "wget" in mock_run.call_args_list[0][0][0]
        assert "gunzip" in mock_run.call_args_list[1][0][0]
        mock_update.assert_called_once_with(bioc_pmc_dir="/tmp/bioc")

    @patch("src.pipeline.runner.aggregate_food_chem_sentences")
    @patch("src.pipeline.runner.run_biobert_filter")
    def test_filtering_runs_biobert_and_aggregate(
        self,
        mock_filter: MagicMock,
        mock_agg: MagicMock,
        settings: IESettings,
    ) -> None:
        runner = IERunner(settings)
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
        settings: IESettings,
    ) -> None:
        runner = IERunner(settings)
        runner._run_extraction()
        mock_extract.assert_called_once()
        mock_agg.assert_called_once()
        mock_json.assert_called_once()


class TestCLI:
    def test_stages_command(self) -> None:
        result = CliRunner().invoke(cli, ["stages"])
        assert result.exit_code == 0
        assert "CORPUS" in result.output
        assert "EXTRACTION" in result.output

    def test_run_help(self) -> None:
        result = CliRunner().invoke(cli, ["run", "--help"])
        assert result.exit_code == 0
        assert "--stages" in result.output

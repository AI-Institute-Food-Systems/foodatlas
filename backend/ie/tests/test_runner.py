"""Tests for the IE pipeline runner and CLI."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from main import cli
from src.runner import IEConfig, IERunner
from src.stages import ALL_STAGES, IEStage

if TYPE_CHECKING:
    from pathlib import Path


class TestIEStage:
    def test_stage_count(self) -> None:
        assert len(IEStage) == 7

    def test_all_stages_sorted(self) -> None:
        values = [s.value for s in ALL_STAGES]
        assert values == list(range(7))

    def test_stage_names(self) -> None:
        assert IEStage.DOWNLOAD_PMC_IDS.value == 0
        assert IEStage.PARSE.value == 6


class TestIEConfig:
    @pytest.fixture()
    def config(self, tmp_path: Path) -> IEConfig:
        return IEConfig(
            date="2026_04_06",
            model="gpt-4",
            pipeline_dir=tmp_path,
            bioc_pmc_dir="/tmp/bioc",
            biobert_model_dir="outputs/biobert",
            food_terms="data/food_terms.txt",
            threshold=0.99,
        )

    def test_run_dir(self, config: IEConfig) -> None:
        expected = config.pipeline_dir / "outputs" / "text_parser" / "2026_04_06"
        assert config.run_dir == expected

    def test_preds_dir(self, config: IEConfig) -> None:
        expected = config.pipeline_dir / "outputs" / "past_sentence_filtering_preds"
        assert config.preds_dir == expected


class TestIERunner:
    @pytest.fixture()
    def config(self, tmp_path: Path) -> IEConfig:
        return IEConfig(
            date="2026_04_06",
            model="gpt-4",
            pipeline_dir=tmp_path,
            bioc_pmc_dir="/tmp/bioc",
            biobert_model_dir="outputs/biobert",
            food_terms="data/food_terms.txt",
            threshold=0.99,
        )

    def test_run_calls_stages_in_order(self, config: IEConfig) -> None:
        runner = IERunner(config)
        called: list[str] = []

        with (
            patch.object(runner, "_step0_download_pmc_ids", lambda: called.append("0")),
            patch.object(runner, "_step1_update_bioc", lambda: called.append("1")),
            patch.object(runner, "_step2_search_pubmed", lambda: called.append("2")),
            patch.object(runner, "_step3_biobert_filter", lambda: called.append("3")),
            patch.object(runner, "_step4_aggregate", lambda: called.append("4")),
            patch.object(runner, "_step5_extract", lambda: called.append("5")),
            patch.object(runner, "_step6_parse", lambda: called.append("6")),
        ):
            runner.run([IEStage.SEARCH_PUBMED, IEStage.BIOBERT_FILTER])

        assert called == ["2", "3"]

    def test_run_all_stages(self, config: IEConfig) -> None:
        runner = IERunner(config)
        called: list[str] = []

        with (
            patch.object(runner, "_step0_download_pmc_ids", lambda: called.append("0")),
            patch.object(runner, "_step1_update_bioc", lambda: called.append("1")),
            patch.object(runner, "_step2_search_pubmed", lambda: called.append("2")),
            patch.object(runner, "_step3_biobert_filter", lambda: called.append("3")),
            patch.object(runner, "_step4_aggregate", lambda: called.append("4")),
            patch.object(runner, "_step5_extract", lambda: called.append("5")),
            patch.object(runner, "_step6_parse", lambda: called.append("6")),
        ):
            runner.run()

        assert called == ["0", "1", "2", "3", "4", "5", "6"]

    @patch("src.runner.subprocess.run")
    def test_step0_downloads_and_extracts(
        self, mock_run: MagicMock, config: IEConfig
    ) -> None:
        runner = IERunner(config)
        ncbi_dir = config.pipeline_dir / "data" / "NCBI"
        ncbi_dir.mkdir(parents=True)

        runner._step0_download_pmc_ids()

        assert mock_run.call_count == 2
        assert "wget" in mock_run.call_args_list[0][0][0]
        assert "gunzip" in mock_run.call_args_list[1][0][0]

    @patch("src.runner.importlib.import_module")
    def test_step1_calls_update_main(
        self, mock_import: MagicMock, config: IEConfig
    ) -> None:
        mock_mod = MagicMock()
        mock_import.return_value = mock_mod

        runner = IERunner(config)
        runner._step1_update_bioc()

        mock_import.assert_called_once_with("src.lit2kg.0_update_PMC_BioC")
        mock_mod.main.assert_called_once()

    @patch("src.runner.importlib.import_module")
    def test_step4_calls_aggregate(
        self, mock_import: MagicMock, config: IEConfig
    ) -> None:
        mock_mod = MagicMock()
        mock_import.return_value = mock_mod

        runner = IERunner(config)
        runner._step4_aggregate()

        mock_import.assert_called_once_with(
            "src.lit2kg.3_aggregate_sentence_filtering_results"
        )
        mock_mod.aggregate_food_chem_sentences.assert_called_once()

    @patch("src.runner.importlib.import_module")
    def test_step6_calls_parse(self, mock_import: MagicMock, config: IEConfig) -> None:
        mock_mod = MagicMock()
        mock_import.return_value = mock_mod

        runner = IERunner(config)
        runner._step6_parse()

        mock_mod.aggregate_batch_predictions.assert_called_once()
        mock_mod.tsv_to_json.assert_called_once()


class TestCLI:
    def test_stages_command(self) -> None:
        result = CliRunner().invoke(cli, ["stages"])
        assert result.exit_code == 0
        assert "DOWNLOAD_PMC_IDS" in result.output
        assert "PARSE" in result.output

    def test_run_help(self) -> None:
        result = CliRunner().invoke(cli, ["run", "--help"])
        assert result.exit_code == 0
        assert "--stages" in result.output
        assert "--model" in result.output

"""Tests for the CLI entry point."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from main import cli
from src.pipeline.stages import PipelineStage


@patch("main.PipelineRunner")
def test_run_no_stages(mock_runner_cls: MagicMock) -> None:
    runner_instance = MagicMock()
    mock_runner_cls.return_value = runner_instance

    result = CliRunner().invoke(cli, ["run"])
    assert result.exit_code == 0
    runner_instance.run.assert_called_once_with(None)


@patch("main.PipelineRunner")
def test_run_single_stage(mock_runner_cls: MagicMock) -> None:
    runner_instance = MagicMock()
    mock_runner_cls.return_value = runner_instance

    result = CliRunner().invoke(cli, ["run", "--stage", "kg_init"])
    assert result.exit_code == 0
    runner_instance.run.assert_called_once_with([PipelineStage.KG_INIT])


@patch("main.PipelineRunner")
def test_run_multiple_stages(mock_runner_cls: MagicMock) -> None:
    runner_instance = MagicMock()
    mock_runner_cls.return_value = runner_instance

    result = CliRunner().invoke(
        cli, ["run", "--stage", "kg_init", "--stage", "postprocessing"]
    )
    assert result.exit_code == 0
    runner_instance.run.assert_called_once_with(
        [PipelineStage.KG_INIT, PipelineStage.POSTPROCESSING]
    )


@patch("main.PipelineRunner")
def test_init_command(mock_runner_cls: MagicMock) -> None:
    runner_instance = MagicMock()
    mock_runner_cls.return_value = runner_instance

    result = CliRunner().invoke(cli, ["init"])
    assert result.exit_code == 0
    runner_instance.run.assert_called_once_with([PipelineStage.KG_INIT])


def test_invalid_stage() -> None:
    result = CliRunner().invoke(cli, ["run", "--stage", "nonexistent"])
    assert result.exit_code != 0


@patch("main.PipelineRunner")
def test_output_format_option(mock_runner_cls: MagicMock) -> None:
    runner_instance = MagicMock()
    mock_runner_cls.return_value = runner_instance

    result = CliRunner().invoke(cli, ["--output-format", "parquet", "run"])
    assert result.exit_code == 0
    settings = mock_runner_cls.call_args[0][0]
    assert settings.output_format == "parquet"


@patch("main.PipelineRunner")
def test_verbose_flag(mock_runner_cls: MagicMock) -> None:
    runner_instance = MagicMock()
    mock_runner_cls.return_value = runner_instance

    result = CliRunner().invoke(cli, ["-v", "run"])
    assert result.exit_code == 0

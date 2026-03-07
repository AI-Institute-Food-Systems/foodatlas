"""Tests for pipeline runner."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from src.models.settings import KGCSettings
from src.pipeline.runner import _STAGE_HANDLERS, PipelineRunner
from src.pipeline.stages import PipelineStage


@pytest.fixture
def settings() -> KGCSettings:
    return KGCSettings(
        kg_dir="/tmp/test_kg",
        data_dir="/tmp/test_data",
        integration_dir="/tmp/test_dp",
        output_dir="/tmp/test_out",
        cache_dir="/tmp/test_cache",
    )


@pytest.fixture
def runner(settings: KGCSettings) -> PipelineRunner:
    return PipelineRunner(settings)


def _noop_handlers() -> dict[PipelineStage, object]:
    return {stage: MagicMock() for stage in PipelineStage}


def test_runner_init(runner: PipelineRunner) -> None:
    assert runner._kg is None


def test_run_stage_calls_handler(runner: PipelineRunner) -> None:
    mock = MagicMock()
    with patch.dict(_STAGE_HANDLERS, {PipelineStage.ONTOLOGY_PREP: mock}):
        runner.run_stage(PipelineStage.ONTOLOGY_PREP)
    mock.assert_called_once_with(runner)


def test_run_selected_stages_in_order(runner: PipelineRunner) -> None:
    called: list[str] = []

    def _make_tracker(name: str):
        def _handler(self: PipelineRunner) -> None:
            called.append(name)

        return _handler

    overrides = {
        PipelineStage.MERGE_FLAVOR: _make_tracker("MERGE_FLAVOR"),
        PipelineStage.KG_INIT: _make_tracker("KG_INIT"),
        PipelineStage.ONTOLOGY_PREP: _make_tracker("ONTOLOGY_PREP"),
    }
    with patch.dict(_STAGE_HANDLERS, overrides):
        runner.run(
            [
                PipelineStage.MERGE_FLAVOR,
                PipelineStage.KG_INIT,
                PipelineStage.ONTOLOGY_PREP,
            ]
        )

    assert called == ["ONTOLOGY_PREP", "KG_INIT", "MERGE_FLAVOR"]


def test_run_all_stages(runner: PipelineRunner) -> None:
    called: list[str] = []

    def _make_tracker(name: str):
        def _handler(self: PipelineRunner) -> None:
            called.append(name)

        return _handler

    overrides = {stage: _make_tracker(stage.name) for stage in PipelineStage}
    with patch.dict(_STAGE_HANDLERS, overrides):
        runner.run()

    assert len(called) == len(PipelineStage)
    values = [PipelineStage[name].value for name in called]
    assert values == sorted(values)


def test_run_all_writes_version(runner: PipelineRunner) -> None:
    overrides = _noop_handlers()
    with (
        patch.dict(_STAGE_HANDLERS, overrides),
        patch.object(runner, "_write_version") as mock_version,
    ):
        runner.run()
        mock_version.assert_called_once()


def test_run_selected_does_not_write_version(runner: PipelineRunner) -> None:
    with (
        patch.dict(_STAGE_HANDLERS, {PipelineStage.KG_INIT: MagicMock()}),
        patch.object(runner, "_write_version") as mock_version,
    ):
        runner.run([PipelineStage.KG_INIT])
        mock_version.assert_not_called()

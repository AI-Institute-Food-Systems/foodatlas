"""Tests for pipeline runner."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from src.models.settings import KGCSettings

if TYPE_CHECKING:
    from pathlib import Path
from src.pipeline.runner import _STAGE_HANDLERS, PipelineRunner
from src.pipeline.stages import PipelineStage


@pytest.fixture
def settings(tmp_path: Path) -> KGCSettings:
    base = tmp_path
    (base / "test_kg").mkdir()
    return KGCSettings(
        kg_dir=str(base / "test_kg"),
        data_dir=str(base / "test_data"),
        pipeline={"stages": {"data_cleaning": {"output_dir": str(base / "test_dp")}}},
        output_dir=str(base / "test_out"),
        cache_dir=str(base / "test_cache"),
    )


@pytest.fixture
def runner(settings: KGCSettings) -> PipelineRunner:
    return PipelineRunner(settings)


def _noop_handlers() -> dict[PipelineStage, object]:
    return {stage: MagicMock() for stage in PipelineStage}


def test_run_stage_calls_handler(runner: PipelineRunner) -> None:
    mock = MagicMock()
    with patch.dict(_STAGE_HANDLERS, {PipelineStage.INGEST: mock}):
        runner.run_stage(PipelineStage.INGEST)
    mock.assert_called_once_with(runner)


def test_run_selected_stages_in_order(runner: PipelineRunner) -> None:
    called: list[str] = []

    def _make_tracker(name: str):
        def _handler(self: PipelineRunner) -> None:
            called.append(name)

        return _handler

    overrides = {
        PipelineStage.ENRICHMENT: _make_tracker("ENRICHMENT"),
        PipelineStage.ENTITIES: _make_tracker("ENTITIES"),
        PipelineStage.INGEST: _make_tracker("INGEST"),
    }
    with patch.dict(_STAGE_HANDLERS, overrides):
        runner.run(
            [
                PipelineStage.ENRICHMENT,
                PipelineStage.ENTITIES,
                PipelineStage.INGEST,
            ]
        )

    assert called == ["INGEST", "ENTITIES", "ENRICHMENT"]


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

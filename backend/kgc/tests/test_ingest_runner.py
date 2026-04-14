"""Tests for IngestRunner."""

from __future__ import annotations

import inspect
import multiprocessing
from typing import Any
from unittest.mock import MagicMock, patch

from src.models.ingest import SourceManifest
from src.models.settings import KGCSettings
from src.pipeline.ingest.protocol import _noop_progress
from src.pipeline.ingest.runner import (
    ALL_ADAPTERS,
    IngestRunner,
    _create_bars,
    _drain_queue,
    _make_queue_callback,
    _run_single_adapter,
)


def test_all_adapters_registered() -> None:
    assert len(ALL_ADAPTERS) == 9


def test_all_adapters_unique_source_ids() -> None:
    ids = [cls().source_id for cls in ALL_ADAPTERS]
    assert len(ids) == len(set(ids))


@patch("src.pipeline.ingest.runner._run_with_progress")
def test_ingest_runner_run(mock_run: MagicMock) -> None:
    settings = KGCSettings(
        data_dir="/tmp/data",
        output_dir="/tmp/out",
    )
    runner = IngestRunner(settings)

    mock_run.return_value = {"test": SourceManifest(source_id="test")}
    results = runner.run()

    assert "test" in results
    mock_run.assert_called_once()


@patch("src.pipeline.ingest.runner._run_with_progress")
def test_ingest_runner_filters_sources(mock_run: MagicMock) -> None:
    settings = KGCSettings(
        data_dir="/tmp/data",
        output_dir="/tmp/out",
    )
    runner = IngestRunner(settings)
    mock_run.return_value = {}

    runner.run(sources=["foodon"])

    call_args = mock_run.call_args
    adapters = call_args[0][0]
    assert len(adapters) == 1
    assert adapters[0]().source_id == "foodon"


def test_all_adapters_accept_progress_callback() -> None:
    """Verify every adapter's ingest() accepts a progress parameter."""
    for cls in ALL_ADAPTERS:
        sig = inspect.signature(cls.ingest)
        assert "progress" in sig.parameters, f"{cls.__name__} missing progress param"


def _make_queue() -> Any:
    return multiprocessing.Queue()


def test_make_queue_callback_throttles() -> None:
    queue = _make_queue()
    cb = _make_queue_callback(queue, "test_source", throttle=3)
    cb(1, 100)
    cb(2, 100)
    cb(3, 100)
    assert queue.qsize() == 1
    assert queue.get() == ("test_source", 3, 100)


def test_make_queue_callback_always_sends_final() -> None:
    queue = _make_queue()
    cb = _make_queue_callback(queue, "test_source", throttle=1000)
    cb(100, 100)
    assert queue.qsize() == 1


def test_noop_progress_does_not_raise() -> None:
    _noop_progress(0, 100)


def test_create_bars_returns_one_per_source() -> None:
    bars = _create_bars(["foodon", "chebi"])
    assert set(bars.keys()) == {"foodon", "chebi"}
    for bar in bars.values():
        assert bar.total == 0
        bar.close()


def test_run_single_adapter_sends_sentinel_on_success() -> None:
    queue = _make_queue()

    class _FakeAdapter:
        @property
        def source_id(self) -> str:
            return "fake"

        def ingest(
            self, raw_dir: Any, output_dir: Any, progress: Any = None
        ) -> SourceManifest:
            return SourceManifest(source_id="fake")

    manifest = _run_single_adapter(_FakeAdapter, "/tmp", "/tmp", queue)
    assert manifest.source_id == "fake"
    assert queue.get(timeout=2) == ("fake", -1, -1)


def test_run_single_adapter_sends_error_sentinel_on_failure() -> None:
    queue = _make_queue()

    class _BadAdapter:
        @property
        def source_id(self) -> str:
            return "bad"

        def ingest(
            self, raw_dir: Any, output_dir: Any, progress: Any = None
        ) -> SourceManifest:
            msg = "boom"
            raise RuntimeError(msg)

    result = _run_single_adapter(_BadAdapter, "/tmp", "/tmp", queue)
    assert isinstance(result, str)
    assert "boom" in result
    assert queue.get(timeout=2) == ("bad", -2, -2)


def test_drain_queue_advances_bars() -> None:
    queue = _make_queue()
    bars = _create_bars(["foodon"])

    queue.put(("foodon", 50, 100))
    queue.put(("foodon", 100, 100))
    queue.put(("foodon", -1, -1))

    _drain_queue(queue, bars, 1)

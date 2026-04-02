"""Tests for IngestRunner."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.models.ingest import SourceManifest
from src.models.settings import KGCSettings
from src.pipeline.ingest.runner import ALL_ADAPTERS, IngestRunner


def test_all_adapters_registered() -> None:
    assert len(ALL_ADAPTERS) == 8


def test_all_adapters_unique_source_ids() -> None:
    ids = [cls().source_id for cls in ALL_ADAPTERS]
    assert len(ids) == len(set(ids))


@patch("src.pipeline.ingest.runner.ProcessPoolExecutor")
def test_ingest_runner_run(mock_pool_cls: MagicMock) -> None:
    settings = KGCSettings(
        data_dir="/tmp/data",
        output_dir="/tmp/out",
    )
    runner = IngestRunner(settings)

    mock_future = MagicMock()
    mock_future.result.return_value = SourceManifest(source_id="test")

    mock_pool = MagicMock()
    mock_pool.__enter__ = MagicMock(return_value=mock_pool)
    mock_pool.__exit__ = MagicMock(return_value=False)
    mock_pool.submit.return_value = mock_future
    mock_pool_cls.return_value = mock_pool

    with patch("src.pipeline.ingest.runner.as_completed", return_value=[mock_future]):
        results = runner.run()

    assert "test" in results

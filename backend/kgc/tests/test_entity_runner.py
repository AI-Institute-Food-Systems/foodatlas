"""Tests for EntityRunner."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from src.models.settings import KGCSettings
from src.pipeline.entities.runner import EntityRunner

if TYPE_CHECKING:
    from pathlib import Path


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


@patch("src.pipeline.entities.runner.load_sources")
@patch("src.pipeline.entities.runner.load_corrections")
@patch("src.pipeline.entities.runner.filter_sources")
@patch("src.pipeline.entities.runner.create_empty_entity_files")
@patch("src.pipeline.entities.runner.EntityResolver")
def test_run_calls_all_steps(
    mock_resolver_cls: MagicMock,
    mock_scaffold: MagicMock,
    mock_filter: MagicMock,
    mock_corrections: MagicMock,
    mock_loader: MagicMock,
    settings: KGCSettings,
) -> None:
    mock_loader.return_value = {"foodon": {"nodes": MagicMock()}}
    corrections = MagicMock()
    mock_corrections.return_value = corrections
    resolver = MagicMock()
    mock_resolver_cls.return_value = resolver

    runner = EntityRunner(settings)
    runner.run()

    mock_loader.assert_called_once_with(settings)
    mock_corrections.assert_called_once()
    mock_filter.assert_called_once()
    mock_scaffold.assert_called_once_with(settings)
    resolver.resolve.assert_called_once()
    resolver.entity_store.save.assert_called_once()


@patch("src.pipeline.entities.runner.load_sources")
@patch("src.pipeline.entities.runner.load_corrections")
@patch("src.pipeline.entities.runner.filter_sources")
@patch("src.pipeline.entities.runner.create_empty_entity_files")
@patch("src.pipeline.entities.runner.EntityResolver")
def test_run_passes_sources_to_filter_and_resolver(
    mock_resolver_cls: MagicMock,
    mock_scaffold: MagicMock,
    mock_filter: MagicMock,
    mock_corrections: MagicMock,
    mock_loader: MagicMock,
    settings: KGCSettings,
) -> None:
    sources = {"chebi": {"nodes": MagicMock()}}
    mock_loader.return_value = sources
    corrections = MagicMock()
    mock_corrections.return_value = corrections
    resolver = MagicMock()
    mock_resolver_cls.return_value = resolver

    runner = EntityRunner(settings)
    runner.run()

    mock_filter.assert_called_once_with(sources, corrections.ontology_roots)
    resolver.resolve.assert_called_once_with(sources)

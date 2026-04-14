"""Tests for src.etl.loader — ETL orchestrator with mocked DB."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from src.etl.loader import load_kg


def _stub_reader(mock_reader: MagicMock) -> None:
    """Configure mock parquet_reader to return empty DataFrames."""
    mock_reader.read_entities.return_value = pd.DataFrame()
    mock_reader.read_relationships.return_value = pd.DataFrame()
    mock_reader.read_triplets.return_value = pd.DataFrame()
    mock_reader.read_evidence.return_value = pd.DataFrame()
    mock_reader.read_attestations.return_value = pd.DataFrame()


class TestLoadKg:
    """Test load_kg orchestrator with mocked dependencies."""

    @patch("src.etl.loader.refresh_search")
    @patch("src.etl.loader.refresh_all")
    @patch("src.etl.loader.bulk_copy")
    @patch("src.etl.loader._recreate_schema")
    @patch("src.etl.loader.parquet_reader")
    def test_calls_read_functions(
        self,
        mock_reader,
        mock_recreate,
        mock_bulk,
        mock_refresh,
        mock_search,
        fixtures_dir: Path,
    ):
        """Verify all parquet reader functions are called."""
        _stub_reader(mock_reader)
        mock_bulk.return_value = 0
        conn = MagicMock()

        load_kg(conn, fixtures_dir)

        resolved = fixtures_dir.resolve()
        mock_reader.read_entities.assert_called_once_with(resolved)
        mock_reader.read_relationships.assert_called_once_with(resolved)
        mock_reader.read_triplets.assert_called_once_with(resolved)
        mock_reader.read_evidence.assert_called_once_with(resolved)
        mock_reader.read_attestations.assert_called_once_with(resolved)

    @patch("src.etl.loader.refresh_search")
    @patch("src.etl.loader.refresh_all")
    @patch("src.etl.loader.bulk_copy")
    @patch("src.etl.loader._recreate_schema")
    @patch("src.etl.loader.parquet_reader")
    def test_recreates_schema(
        self,
        mock_reader,
        mock_recreate,
        mock_bulk,
        mock_refresh,
        mock_search,
        fixtures_dir: Path,
    ):
        """Verify schema is recreated before insert."""
        _stub_reader(mock_reader)
        mock_bulk.return_value = 0
        conn = MagicMock()

        load_kg(conn, fixtures_dir)

        mock_recreate.assert_called_once_with(conn)

    @patch("src.etl.loader.refresh_search")
    @patch("src.etl.loader.refresh_all")
    @patch("src.etl.loader.bulk_copy")
    @patch("src.etl.loader._recreate_schema")
    @patch("src.etl.loader.parquet_reader")
    def test_bulk_copy_called_for_each_table(
        self,
        mock_reader,
        mock_recreate,
        mock_bulk,
        mock_refresh,
        mock_search,
        fixtures_dir: Path,
    ):
        """Verify bulk_copy is called 5 times (one per table)."""
        _stub_reader(mock_reader)
        mock_bulk.return_value = 0
        conn = MagicMock()

        load_kg(conn, fixtures_dir)

        assert mock_bulk.call_count == 5
        table_names = [c.args[1] for c in mock_bulk.call_args_list]
        assert "base_entities" in table_names
        assert "relationships" in table_names
        assert "base_triplets" in table_names
        assert "base_evidence" in table_names
        assert "base_attestations" in table_names

    @patch("src.etl.loader.refresh_search")
    @patch("src.etl.loader.refresh_all")
    @patch("src.etl.loader.bulk_copy")
    @patch("src.etl.loader._recreate_schema")
    @patch("src.etl.loader.parquet_reader")
    def test_materializers_called(
        self,
        mock_reader,
        mock_recreate,
        mock_bulk,
        mock_refresh,
        mock_search,
        fixtures_dir: Path,
    ):
        """Verify both materialize functions are called."""
        _stub_reader(mock_reader)
        mock_bulk.return_value = 0
        conn = MagicMock()

        load_kg(conn, fixtures_dir)

        mock_refresh.assert_called_once_with(conn)
        mock_search.assert_called_once_with(conn)

    @patch("src.etl.loader.refresh_search")
    @patch("src.etl.loader.refresh_all")
    @patch("src.etl.loader.bulk_copy")
    @patch("src.etl.loader._recreate_schema")
    @patch("src.etl.loader.parquet_reader")
    def test_commits_after_insert(
        self,
        mock_reader,
        mock_recreate,
        mock_bulk,
        mock_refresh,
        mock_search,
        fixtures_dir: Path,
    ):
        """Verify conn.commit() is called after bulk insert."""
        _stub_reader(mock_reader)
        mock_bulk.return_value = 0
        conn = MagicMock()

        load_kg(conn, fixtures_dir)

        conn.commit.assert_called_once()

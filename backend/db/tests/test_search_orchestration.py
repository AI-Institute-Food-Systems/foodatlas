"""Tests for src.etl.materializer_search — orchestration with mocked DB."""

from unittest.mock import MagicMock, patch

from src.etl.materializer_search import refresh_search


class TestRefreshSearch:
    """Test refresh_search orchestration."""

    @patch("src.etl.materializer_search._materialize_statistics")
    @patch("src.etl.materializer_search._materialize_search_auto_complete")
    @patch("src.etl.materializer_search.truncate_tables")
    def test_calls_truncate_then_materialize(
        self,
        mock_truncate,
        mock_search,
        mock_stats,
    ):
        conn = MagicMock()
        refresh_search(conn)
        mock_truncate.assert_called_once_with(
            conn, ["mv_search_auto_complete", "mv_metadata_statistics"]
        )
        mock_search.assert_called_once_with(conn)
        mock_stats.assert_called_once_with(conn)
        conn.commit.assert_called_once()
